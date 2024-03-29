namespace AzureTackle.Analyzers.Core

open System
open F23.StringSimilarity
open InformationSchema
open FSharp.Control.Tasks.ContextInsensitive
open FSharp.Core
open FSharp.Compiler.Text

module AzureAnalysis =


    type ComparisonFilters =
        { filterName: string
          column: string
          table: string option }
    let columnFilterTable (filterName: string) (columnName: string) =
        if columnName.Contains "." then
            let parts = columnName.Split '.'

            { filterName = filterName
              column = parts.[1]
              table = Some parts.[0] }
        else
            { filterName = filterName
              column = columnName
              table = None }

    let extractTableInfo (connectionString, tableName, avaiableTables) =
        task {
            try
                let! tableInfo = extractTableInfo (connectionString, tableName, avaiableTables)
                return Result.Ok tableInfo
            with error ->
                // any other generic error
                return Result.Error(sprintf "%s\n%s" error.Message error.StackTrace)
        }

    let createWarning (analysisType:AnalysisType) (message: string) range: Message =
        printfn "Warning %s" message

        { Message = message
          Type = analysisType.GetValue
          Code = "AZUREL0001"
          Severity = Warning
          Range = range
          Fixes = [] }

    let createInfo (analysisType:AnalysisType) (message: string) (range: range): Message =
        printfn "Info %s" message

        { Message = message
          Type = analysisType.GetValue
          Code = "AZURE0001"
          Severity = Info
          Range = range
          Fixes = [] }

    let findTable (operation: AzureOperation) =
        operation.blocks
        |> List.tryFind
            (function
            | AzureAnalyzerBlock.Table (table, range) -> true
            | _ -> false)
        |> Option.map
            (function
            | AzureAnalyzerBlock.Table (table, range) -> (table, range)
            | _ -> failwith "should not happen")


    let findFilters (operation: AzureOperation) =
        operation.blocks
        |> List.tryFind
            (function
            | AzureAnalyzerBlock.Filters (filters, range) -> true
            | _ -> false)
        |> Option.map
            (function
            | AzureAnalyzerBlock.Filters (filters, range) -> (filters, range)
            | _ -> failwith "should not happen")

    let findColumnReadAttempts (operation: AzureOperation) =
        operation.blocks
        |> List.tryFind
            (function
            | AzureAnalyzerBlock.ReadingColumns (attempts) -> true
            | _ -> false)
        |> Option.map
            (function
            | AzureAnalyzerBlock.ReadingColumns (attempts) -> attempts
            | _ -> failwith "should not happen")

    let analyzeFilter (operation: AzureOperation) (requiredFilters: InformationSchema.TableInfo list) =
        printfn "analyze filter %A" requiredFilters

        match findFilters operation with
        | None ->
            if not (List.isEmpty requiredFilters) then
                let missingFilters =
                    requiredFilters
                    |> List.map (fun f -> sprintf "%s:%A" f.ColumnName f.EntityProperty.PropertyType)
                    |> String.concat ", "
                    |> sprintf "You didn't assign a filter. You can filter following properties [%s]. Please use AzureTable.filter to provide them."

                [ createInfo Filter missingFilters operation.range ]
            else
                []

        | Some (queryFilters, queryFiltersRange) ->
            if List.isEmpty requiredFilters then
                [ createWarning Filter "Provided filters are redundant. Azure execute is not parameterized" operation.range ]
            else

                /// None of the required filters have the name of this provided filter
                let isUnknown (filters: UsedFilter) =
                    requiredFilters
                    |> List.forall (fun requiredFilter -> filters.name <> requiredFilter.ColumnName)

                let isRedundant (filter: UsedFilter) =
                    // every required filter has a corresponding provided query filter
                    let requirementSatisfied =
                        requiredFilters
                        |> List.forall
                            (fun requiredFilter ->
                                queryFilters
                                |> List.exists (fun queryFilter -> queryFilter.name = requiredFilter.ColumnName))

                    requirementSatisfied && isUnknown filter

                [ for requiredFilter in requiredFilters do
                    if not (
                            queryFilters
                            |> List.exists (fun providedFilter -> providedFilter.name = requiredFilter.ColumnName)
                    ) then
                        let message =
                            sprintf
                                "Missing filter '%s' of type %A"
                                requiredFilter.ColumnName
                                requiredFilter.EntityProperty.PropertyType

                        yield createWarning Filter message queryFiltersRange

                  for providedFilter in queryFilters do
                      if isRedundant providedFilter then
                          yield
                              createWarning Filter
                                  (sprintf
                                      "Provided filter '%s' is redundant. The query does not require such filter"
                                      providedFilter.name)
                                  providedFilter.range

                      else

                      if isUnknown providedFilter then

                          // filters that haven't been provided yet
                          let remainingFilters =
                              requiredFilters
                              |> List.filter
                                  (fun providedFilter ->
                                      not (
                                          queryFilters
                                          |> List.exists (fun queryParam -> queryParam.name = providedFilter.ColumnName)
                                      ))

                          let levenshtein = NormalizedLevenshtein()

                          let closestAlternative =
                              remainingFilters
                              |> List.minBy (fun filter -> levenshtein.Distance(filter.ColumnName, providedFilter.name))
                              |> fun filter -> filter.ColumnName

                          let expectedFilters =
                              remainingFilters
                              |> List.map (fun p -> sprintf "%s:%A" p.ColumnName p.EntityProperty.PropertyType)
                              |> String.concat ", "
                              |> sprintf "Expected filters are [%s]."

                          let codeFixes =
                              remainingFilters
                              |> List.map
                                  (fun p ->
                                      { FromRange = providedFilter.range
                                        FromText = providedFilter.name
                                        ToText = sprintf "\"%s\"" p.ColumnName })

                          let warning =
                              if String.IsNullOrWhiteSpace(providedFilter.name) then
                                  createWarning Filter
                                      (sprintf
                                          "Empty filter name was provided. Please provide one of %s"
                                          expectedFilters)
                                      providedFilter.range
                              else if List.length codeFixes = 1 then
                                  createWarning Filter
                                      (sprintf
                                          "Unexpected filter '%s' is provided. Did you mean '%s'?"
                                          providedFilter.name
                                          closestAlternative)
                                      providedFilter.range
                              else
                                  createWarning Filter
                                      (sprintf
                                          "Unexpected filter '%s' is provided. Did you mean '%s'? %s"
                                          providedFilter.name
                                          closestAlternative
                                          expectedFilters)
                                      providedFilter.range

                          yield { warning with Fixes = codeFixes }
                      else
                          // do filter type-checking
                          let matchingColumnType =
                              requiredFilters
                              |> List.tryFind (fun p -> p.ColumnName = providedFilter.name)

                          match matchingColumnType with
                          | None -> ()
                          | Some requiredFilter ->
                              let typeMismatch (shouldUse: string list) =
                                  let formattedSuggestions =
                                      match shouldUse with
                                      | [] -> "<empty>"
                                      | [ first ] -> first
                                      | [ first; second ] -> sprintf "%s or %s" first second
                                      | _ ->
                                          let lastSuggestion = List.last shouldUse

                                          let firstSuggestions =
                                              shouldUse
                                              |> List.rev
                                              |> List.skip 1
                                              |> List.rev
                                              |> String.concat ", "

                                          sprintf "%s or %s" firstSuggestions lastSuggestion

                                  let warning =
                                      sprintf
                                          "Attempting to provide filter '%s' of type '%s' using function %s. Please use %s instead."
                                          requiredFilter.ColumnName
                                          (requiredFilter.EntityProperty.PropertyType.ToString())
                                          providedFilter.filterFunc
                                          formattedSuggestions

                                  let codeFixs: Fix list =
                                      shouldUse
                                      |> List.map
                                          (fun func ->
                                              { FromText = providedFilter.name
                                                FromRange = providedFilter.filterFuncRange
                                                ToText = func })

                                  { createWarning Filter warning providedFilter.filterFuncRange with
                                        Fixes = codeFixs }

                              if providedFilter.filterFunc = "AzureTable.filter" then
                                  // do not do anything when the input is a generic param
                                  ()
                              else
                                  let entity = requiredFilter.EntityProperty.ToString()

                                  match entity with

                                  | ("bool"
                                  | "boolean") ->
                                      if providedFilter.filterFunc <> "AzureTable.bool"
                                      then yield typeMismatch [ "AzureTable.bool" ]

                                  | ("int"
                                  | "int32"
                                  | "integer"
                                  | "serial") ->
                                      if providedFilter.filterFunc <> "AzureTable.int"
                                      then yield typeMismatch [ "AzureTable.int" ]

                                  | ("smallint"
                                  | "int16") ->
                                      if providedFilter.filterFunc <> "AzureTable.int16"
                                      then yield typeMismatch [ "AzureTable.int16" ]

                                  | ("int64"
                                  | "bigint"
                                  | "bigserial") ->
                                      if providedFilter.filterFunc <> "AzureTable.int64"
                                         && providedFilter.filterFunc <> "AzureTable.int" then
                                          yield
                                              typeMismatch [ "AzureTable.int64"
                                                             "AzureTable.int" ]

                                  | ("numeric"
                                  | "decimal"
                                  | "money") ->
                                      if providedFilter.filterFunc <> "AzureTable.decimal"
                                      then yield typeMismatch [ "AzureTable.decimal" ]

                                  | "double precision" ->
                                      if providedFilter.filterFunc <> "AzureTable.double"
                                      then yield typeMismatch [ "AzureTable.double" ]

                                  | "bytea" ->
                                      if providedFilter.filterFunc <> "AzureTable.bytea"
                                      then yield typeMismatch [ "AzureTable.bytea" ]

                                  | "uuid" ->
                                      if providedFilter.filterFunc <> "AzureTable.uuid"
                                      then yield typeMismatch [ "AzureTable.uuid" ]

                                  | "date" ->
                                      if providedFilter.filterFunc <> "AzureTable.date"
                                      then yield typeMismatch [ "AzureTable.date" ]

                                  | ("timestamp"
                                  | "timestamp without time zone") ->
                                      if providedFilter.filterFunc
                                         <> "AzureTable.timestamp" then
                                          yield typeMismatch [ "AzureTable.timestamp" ]

                                  | ("timestamptz"
                                  | "timestamp with time zone") ->
                                      if providedFilter.filterFunc
                                         <> "AzureTable.timestamptz" then
                                          yield typeMismatch [ "AzureTable.timestamptz" ]

                                  | "jsonb" ->
                                      if providedFilter.filterFunc <> "AzureTable.jsonb"
                                      then yield typeMismatch [ "AzureTable.jsonb" ]


                                  | _ -> () ]

    let analyzeTable (operation: AzureOperation) (availableTables: InformationSchema.TableList) =
        printfn "found following tables %A" availableTables.Tables

        match findTable operation with
        | None ->

            if not (List.isEmpty availableTables.Tables) then
                let missingTables =
                    availableTables.Tables
                    |> List.map (fun f -> sprintf "%s" f.Name)
                    |> String.concat ", "
                    |> sprintf "Missing tables [%s]. Please use AzureTable.table to provide them."

                [ createWarning Table missingTables operation.range ]
            else
                let message =
                    availableTables.Tables
                    |> List.map (fun f -> sprintf "%s" f.Name)
                    |> String.concat ", "
                    |> sprintf "TableName is not correct please try one of following tables [%s]."

                [ createWarning Table message operation.range ]

        | Some (queryTable, queryTableRange) -> [ createInfo Table queryTable queryTableRange ]



    let findColumn (name: string) (availableColumns: InformationSchema.TableInfo list) =
        availableColumns
        |> List.tryFind (fun column -> column.ColumnName = name)

    let formatColumns (availableColumns: InformationSchema.TableInfo list) =
        availableColumns
        |> List.map (fun column -> sprintf "| -- %s of type %A" column.ColumnName column.EntityProperty.PropertyType)
        |> String.concat "\n"

    let analyzeColumnReadingAttempts (columnReadAttempts: ColumnReadAttempt list)
                                     (availableColumns: InformationSchema.TableInfo list)
                                     =
        [ for attempt in columnReadAttempts do
            match findColumn attempt.columnName availableColumns with
            | None ->
                if List.isEmpty availableColumns then
                    let warningMsg =
                        sprintf
                            "Attempting to read column named '%s' from a result set which doesn't return any columns. In case you are executing DELETE, INSERT or UPDATE queries, you might want to use AzureTable.executeNonQuery or AzureTable.executeNonQueryAsync to obtain the number of affected rows. You can also add a RETURNING clause in your query to make it return the rows which are updated, inserted or deleted from that query."
                            attempt.columnName

                    yield createWarning Execute warningMsg attempt.columnNameRange
                else
                    let levenshtein = NormalizedLevenshtein()

                    let closestAlternative =
                        availableColumns
                        |> List.minBy (fun column -> levenshtein.Distance(attempt.columnName, column.ColumnName))
                        |> fun column -> column.ColumnName

                    let warningMsg =
                        if String.IsNullOrWhiteSpace attempt.columnName then
                            sprintf
                                "Attempting to read a column without specifying a name. Available columns returned from the result set are:\n%s"
                                (formatColumns availableColumns)
                        else
                            sprintf
                                "Attempting to read column named '%s' that was not returned by the result set. Did you mean to read '%s'?\nAvailable columns from the result set are:\n%s"
                                attempt.columnName
                                closestAlternative
                                (formatColumns availableColumns)

                    let warning =
                        createWarning Execute warningMsg attempt.columnNameRange

                    let codeFixes =
                        availableColumns
                        |> List.map
                            (fun column ->
                                { FromRange = attempt.columnNameRange
                                  FromText = attempt.columnName
                                  ToText = sprintf "\"%s\"" column.ColumnName })

                    yield { warning with Fixes = codeFixes }

            | Some column ->
                let typeMismatchMessage (shouldUse: string list) =
                    let formattedFunctions =
                        match shouldUse with
                        | [] -> "<empty>"
                        | [ first ] -> first
                        | [ first; second ] -> sprintf "%s or %s" first second
                        | _ ->
                            let lastSuggestion = List.last shouldUse

                            let firstSuggestions =
                                shouldUse
                                |> List.rev
                                |> List.skip 1
                                |> List.rev
                                |> String.concat ", "

                            sprintf "%s or %s" firstSuggestions lastSuggestion

                    String.concat
                        String.Empty
                        [ sprintf "Type mismatch: attempting to read column '%s' of " column.ColumnName
                          sprintf
                              "type '%s' using %s. "
                              (column.EntityProperty.PropertyType.ToString())
                              attempt.funcName
                          sprintf "Please use %s instead" formattedFunctions ]

                let typeMismatch (shouldUse: string list) =
                    let warningMessage = typeMismatchMessage shouldUse

                    let fixes =
                        shouldUse
                        |> List.map
                            (fun func ->
                                { FromRange = attempt.funcCallRange
                                  FromText = attempt.funcName
                                  ToText = func })

                    { createWarning Execute warningMessage attempt.funcCallRange with
                          Fixes = fixes }

                if attempt.funcName.StartsWith "AzureTable." then
                    match column.EntityProperty.ToString() with
                    | ("bit"
                    | "bool"
                    | "boolean") when attempt.funcName <> "AzureTable.readBool" ->
                        yield typeMismatch [ "AzureTable.readBool" ]
                    | ("character varying"
                    | "character"
                    | "char"
                    | "varchar"
                    | "citext") when attempt.funcName <> "AzureTable.readString" ->
                        yield typeMismatch [ "AzureTable.readString" ]
                    | ("int"
                    | "int2"
                    | "int4"
                    | "smallint"
                    | "integer") when attempt.funcName <> "AzureTable.readInt" ->
                        yield typeMismatch [ "AzureTable.readInt" ]
                    | ("int8"
                    | "bigint") when attempt.funcName <> "AzureTable.readLong" ->
                        yield typeMismatch [ "AzureTable.readLong" ]
                    | ("real"
                    | "float4"
                    | "double precision"
                    | "float8") when attempt.funcName <> "AzureTable.readNumber" ->
                        yield typeMismatch [ "AzureTable.readNumber" ]
                    | ("numeric"
                    | "decimal"
                    | "money") when attempt.funcName <> "AzureTable.readDecimal"
                                    || attempt.funcName <> "AzureTable.readMoney" ->
                        yield typeMismatch [ "AzureTable.readDecimal" ]
                    | "bytea" when attempt.funcName <> "AzureTable.readBytea" ->
                        yield typeMismatch [ "AzureTable.readBytea" ]
                    | "uuid" when attempt.funcName <> "AzureTable.readUuid" ->
                        yield typeMismatch [ "AzureTable.readUuid" ]
                    | "date" when attempt.funcName <> "AzureTable.readDate" ->
                        yield typeMismatch [ "AzureTable.readDate" ]
                    | _ -> ()
                else
                    let replace (name: string) =
                        match attempt.funcName.Split '.' with
                        | [| reader; funcName |] -> sprintf "%s.%s" reader name
                        | _ -> attempt.funcName

                    let using (name: string) =
                        attempt.funcName.EndsWith(sprintf ".%s" name)

                    let notUsing = using >> not

                    match column.EntityProperty.ToString() with
                    | ("bit"
                    | "bool"
                    | "boolean") ->
                        if column.Nullable && notUsing "boolOrNone" then
                            yield typeMismatch [ replace "boolOrNone" ]
                        else if notUsing "boolOrNone" && notUsing "bool" then
                            if column.Nullable
                            then yield typeMismatch [ replace "boolOrNone" ]
                            else yield typeMismatch [ replace "bool" ]
                        else
                            ()

                    | ("int8"
                    | "tinyint") ->
                        if column.Nullable
                           && List.forall
                               notUsing
                               [ "int8OrNone"
                                 "int16OrNone"
                                 "intOrNone"
                                 "int64OrNone" ] then
                            yield
                                typeMismatch [ replace "int8OrNone"
                                               replace "int16OrNone"
                                               replace "intOrNone"
                                               replace "int64OrNone" ]
                        else if List.forall
                                    notUsing
                                    [ "int8OrNone"
                                      "int16OrNone"
                                      "intOrNone"
                                      "int64OrNone"
                                      "int8"
                                      "int16"
                                      "int"
                                      "int64" ] then
                            if column.Nullable then
                                yield
                                    typeMismatch [ replace "int8OrNone"
                                                   replace "int16OrNone"
                                                   replace "intOrNone"
                                                   replace "int64OrNone" ]
                            else
                                yield
                                    typeMismatch [ replace "int8"
                                                   replace "int16"
                                                   replace "int"
                                                   replace "int64" ]

                    | ("int16"
                    | "smallint") ->
                        if column.Nullable
                           && List.forall
                               notUsing
                               [ "int16OrNone"
                                 "intOrNone"
                                 "int64OrNone" ] then
                            yield
                                typeMismatch [ replace "int16OrNone"
                                               replace "intOrNone"
                                               replace "int64OrNone" ]
                        else if List.forall
                                    notUsing
                                    [ "int16OrNone"
                                      "intOrNone"
                                      "int64OrNone"
                                      "int16"
                                      "int"
                                      "int64" ] then
                            if column.Nullable then
                                yield
                                    typeMismatch [ replace "int16OrNone"
                                                   replace "intOrNone"
                                                   replace "int64OrNone" ]
                            else
                                yield
                                    typeMismatch [ replace "int16"
                                                   replace "int"
                                                   replace "int64" ]

                    | ("int"
                    | "integer"
                    | "int32"
                    | "serial"
                    | "int4"
                    | "int2") ->
                        if column.Nullable
                           && List.forall notUsing [ "intOrNone"; "int64OrNone" ] then
                            yield
                                typeMismatch [ replace "intOrNone"
                                               replace "int64OrNone" ]
                        else if List.forall
                                    notUsing
                                    [ "intOrNone"
                                      "int64OrNone"
                                      "int"
                                      "int64" ] then
                            if column.Nullable then
                                yield
                                    typeMismatch [ replace "intOrNone"
                                                   replace "int64OrNone" ]
                            else
                                yield
                                    typeMismatch [ replace "int"
                                                   replace "int64" ]

                    | ("int64"
                    | "bigint"
                    | "bigserial") ->
                        if column.Nullable && notUsing "int64OrNone" then
                            yield typeMismatch [ replace "int64OrNone" ]
                        else if notUsing "int64OrNone" && notUsing "int64" then
                            if column.Nullable
                            then yield typeMismatch [ replace "int64OrNone" ]
                            else yield typeMismatch [ replace "int64" ]
                        else
                            ()

                    | ("numeric"
                    | "decimal"
                    | "money") ->
                        if column.Nullable && notUsing "decimalOrNone" then
                            yield typeMismatch [ replace "decimalOrNone" ]
                        else if notUsing "decimalOrNone" && notUsing "decimal" then
                            if column.Nullable
                            then yield typeMismatch [ replace "decimalOrNone" ]
                            else yield typeMismatch [ replace "decimal" ]
                        else
                            ()

                    | "double precision" ->
                        if column.Nullable && notUsing "doubleOrNone" then
                            yield typeMismatch [ replace "doubleOrNone" ]
                        else if notUsing "doubleOrNone" && notUsing "double" then
                            if column.Nullable
                            then yield typeMismatch [ replace "doubleOrNone" ]
                            else yield typeMismatch [ replace "double" ]
                        else
                            ()

                    | ("text"
                    | "json"
                    | "xml"
                    | "jsonb"
                    | "varchar") ->
                        if column.Nullable
                           && notUsing "textOrNone"
                           && notUsing "stringOrNone" then
                            yield
                                typeMismatch [ replace "textOrNone"
                                               replace "stringOrNone" ]
                        else if notUsing "textOrNone"
                                && notUsing "text"
                                && notUsing "string"
                                && notUsing "stringOrNone" then
                            if column.Nullable then
                                yield
                                    typeMismatch [ replace "textOrNone"
                                                   replace "stringOrText" ]
                            else
                                yield
                                    typeMismatch [ replace "text"
                                                   replace "string" ]
                        else
                            ()

                    | "date" ->
                        if column.Nullable && notUsing "dateOrNone" then
                            yield typeMismatch [ replace "dateOrNone" ]
                        else if notUsing "dateOrNone" && notUsing "date" then
                            if column.Nullable
                            then yield typeMismatch [ replace "dateOrNone" ]
                            else yield typeMismatch [ replace "date" ]
                        else
                            ()

                    | ("timestamp"
                    | "timestamp without time zone") ->
                        if column.Nullable
                           && notUsing "timestampOrNone"
                           && notUsing "dateTimeOrNone" then
                            yield
                                typeMismatch [ replace "dateTimeOrNone"
                                               replace "timestampOrNone" ]
                        else if notUsing "timestampOrNone"
                                && notUsing "timestamp"
                                && notUsing "dateTimeOrNone"
                                && notUsing "dateTime" then
                            if column.Nullable then
                                yield
                                    typeMismatch [ replace "dateTimeOrNone"
                                                   replace "timestampOrNone" ]
                            else
                                yield
                                    typeMismatch [ replace "dateTime"
                                                   replace "timestamp" ]
                        else
                            ()

                    | ("timestamptz"
                    | "timestamp with time zone") ->
                        if column.Nullable
                           && notUsing "timestamptzOrNone"
                           && notUsing "dateTimeOrNone"
                           && notUsing "datetimeOffsetOrNone" then
                            yield
                                typeMismatch [ replace "datetimeOffsetOrNone"
                                               replace "dateTimeOrNone"
                                               replace "timestamptzOrNone" ]
                        //else if not column.Nullable && (using "timestamptzOrNone" || using "dateTimeOrNone")
                        //then yield typeMismatch [ replace "dateTime"; replace "timestamptz" ]
                        else if notUsing "timestamptzOrNone"
                                && notUsing "timestamptz"
                                && notUsing "dateTimeOrNone"
                                && notUsing "dateTime"
                                && notUsing "datetimeOffsetOrNone"
                                && notUsing "datetimeOffset" then
                            if column.Nullable then
                                yield
                                    typeMismatch [ replace "datetimeOffsetOrNone"
                                                   replace "dateTimeOrNone"
                                                   replace "timestamptzOrNone" ]
                            else
                                yield
                                    typeMismatch [ replace "datetimeOffset"
                                                   replace "dateTime"
                                                   replace "timestamptz" ]
                        else
                            ()

                    | "bytea" ->
                        if column.Nullable && notUsing "byteaOrNone" then
                            yield typeMismatch [ replace "byteaOrNone" ]
                        else if notUsing "byteaOrNone" && notUsing "bytea" then
                            if column.Nullable
                            then yield typeMismatch [ replace "byteaOrNone" ]
                            else yield typeMismatch [ replace "bytea" ]
                        else
                            ()
                    | _ -> () ]

    /// Tries to read the database schema from the connection string
    let databaseSchema (connectionString: string) =
        task {
            try
                let! tables = getAzureTables (connectionString)
                return Ok(tables)
            with ex -> return Result.Error("Can't get tables exn: " + ex.Message)
        }
    /// Tries to read the database schema from the connection string
    let entities connectionString tableName =
        task {
            try
                let! entities = getAzureTableEntity (connectionString, tableName)
                return Ok(entities)
            with ex -> return Result.Error ex.Message
        }

    /// Uses database schema that is retrieved once during initialization
    /// and re-used when analyzing the rest of the Sql operation blocks
    let analyzeOperation (operation: AzureOperation) (connectionString: string) =
        task {
            let! tables = databaseSchema (connectionString)

            match tables with
            | Ok tableList ->

                let tableNames =
                    tableList.Tables
                    |> List.map (fun f -> sprintf "%s" f.Name)
                    |> String.concat ", "

                match findTable operation with
                | None ->
                    let message =
                        tableNames
                        |> sprintf "TableName is not correct please try one of following tables [%s]."

                    return [ createWarning Table message operation.range ]
                | Some (table, tableRange) ->
                    let! queryAnalysis = extractTableInfo (connectionString, table, tableNames)

                    match queryAnalysis with
                    | Result.Error queryError -> return [ createWarning Table queryError tableRange ]
                    | Result.Ok tableInfos ->
                        printfn "tableInfos %A" tableInfos
                        let readingAttempts =
                            defaultArg (findColumnReadAttempts operation) []

                        return
                            [ yield! analyzeTable operation tableList
                              yield! analyzeFilter operation tableInfos
                              yield! analyzeColumnReadingAttempts readingAttempts tableInfos ]
            | Result.Error error -> return [ createWarning Other error operation.range ]

        }
        |> Async.AwaitTask
        |> Async.RunSynchronously
