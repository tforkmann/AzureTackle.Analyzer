namespace AzureTackle.Analyzers.Core

open System
open FSharp.Compiler.Range
open F23.StringSimilarity
open InformationSchema
open FSharp.Control.Tasks.ContextInsensitive

module AzureAnalysis =

    type ComparisonFilters = {
        filterName : string
        column : string
        table : string option
    }

    let columnFilterTable (filterName: string) (columnName: string) =
        if columnName.Contains "." then
            let parts = columnName.Split '.'
            { filterName = filterName; column = parts.[1]; table = Some parts.[0] }
        else
            { filterName = filterName; column = columnName; table = None }

    let extractFiltersAndOutputColumns(connectionString, tableName) =
        task {
            try
            let! tableInfo = InformationSchema.extractTableInfo(connectionString, tableName)
            return Result.Ok tableInfo
            with
            | error ->
                // any other generic error
                return Result.Error (sprintf "%s\n%s" error.Message error.StackTrace)
        }

    let createWarning (message: string) (range: range) : Message =
        { Message = message;
          Type = "Azure Analysis";
          Code = "AZUREL0001";
          Severity = Warning;
          Range = range;
          Fixes = [ ] }
    let createInfo (message: string) (range: range) : Message =
        { Message = message;
          Type = "Azure Analysis";
          Code = "AZURE0001";
          Severity = Info;
          Range = range;
          Fixes = [ ] }

    let findTable (operation: AzureOperation) =
        operation.blocks
        |> List.tryFind (function | AzureAnalyzerBlock.Table(table, range) -> true | _ -> false)
        |> Option.map(function | AzureAnalyzerBlock.Table(table, range) -> (table, range) | _ -> failwith "should not happen")


    let findFilters (operation: AzureOperation) =
        operation.blocks
        |> List.tryFind (function | AzureAnalyzerBlock.Filters(filters, range) -> true | _ -> false)
        |> Option.map(function | AzureAnalyzerBlock.Filters(filters, range) -> (filters, range) | _ -> failwith "should not happen")

    let findColumnReadAttempts (operation: AzureOperation) =
        operation.blocks
        |> List.tryFind (function | AzureAnalyzerBlock.ReadingColumns(attempts) -> true | _ -> false)
        |> Option.map(function | AzureAnalyzerBlock.ReadingColumns(attempts) -> attempts | _ -> failwith "should not happen")

    let analyzeFilter filters filtersRange (requiredFilters: InformationSchema.Filters list) =
        match filters with
        | None ->
            if not (List.isEmpty requiredFilters) then
                let missingParameters =
                    requiredFilters
                    |> List.map (fun p -> sprintf "%s:%A" p.Name p.EntityProperty)
                    |> String.concat ", "
                    |> sprintf "Missing parameters [%s]. Please use AzureTable.filter to provide them."
                [ createWarning missingParameters filtersRange ]
            else
                [ ]

        | Some (queryParams, queryParamsRange) ->
            if List.isEmpty requiredFilters then
                [ createWarning "Provided parameters are redundant. Azure execute is not parameterized" filtersRange ]
            else

                /// None of the required parameters have the name of this provided parameter
                let isUnknown (parameter: UsedFilter) =
                    requiredFilters
                    |> List.forall (fun requiredParam -> parameter.name <> requiredParam.Name)

                let isRedundant (parameter: UsedFilter) =
                    // every required parameter has a corresponding provided query parameter
                    let requirementSatisfied =
                        requiredFilters
                        |> List.forall (fun requiredParam -> queryParams |> List.exists (fun queryParam -> queryParam.name = requiredParam.Name))

                    requirementSatisfied && isUnknown parameter

                [
                    for requiredParameter in requiredFilters do
                        if not (queryParams |> List.exists (fun providedParam -> providedParam.name = requiredParameter.Name))
                        then
                            let message = sprintf "Missing parameter '%s' of type %A" requiredParameter.Name requiredParameter.EntityProperty
                            yield createWarning message queryParamsRange

                    for providedParam in queryParams do
                        if isRedundant providedParam then
                            yield createWarning (sprintf "Provided parameter '%s' is redundant. The query does not require such parameter" providedParam.name) providedParam.range

                        else if isUnknown providedParam then

                            // parameters that haven't been provided yet
                            let remainingParameters =
                                requiredFilters
                                |> List.filter (fun requiredParam -> not (queryParams |> List.exists (fun queryParam -> queryParam.name = requiredParam.Name)))

                            let levenshtein = NormalizedLevenshtein()
                            let closestAlternative =
                                remainingParameters
                                |> List.minBy (fun parameter -> levenshtein.Distance(parameter.Name, providedParam.name))
                                |> fun parameter -> parameter.Name

                            let expectedParameters =
                                remainingParameters
                                |> List.map (fun p -> sprintf "%s:%A" p.Name p.EntityProperty)
                                |> String.concat ", "
                                |> sprintf "Required parameters are [%s]."

                            let codeFixes =
                                remainingParameters
                                |> List.map (fun p ->
                                    { FromRange = providedParam.range
                                      FromText = providedParam.name
                                      ToText = sprintf "\"%s\"" p.Name })

                            let warning =
                                if String.IsNullOrWhiteSpace(providedParam.name)
                                then createWarning (sprintf "Empty parameter name was provided. Please provide one of %s" expectedParameters) providedParam.range
                                else if List.length codeFixes = 1
                                then createWarning (sprintf "Unexpected parameter '%s' is provided. Did you mean '%s'?" providedParam.name closestAlternative) providedParam.range
                                else createWarning (sprintf "Unexpected parameter '%s' is provided. Did you mean '%s'? %s" providedParam.name closestAlternative expectedParameters) providedParam.range

                            yield { warning with Fixes = codeFixes }
                        else
                            // do parameter type-checking
                            let matchingColumnType = requiredFilters |> List.tryFind(fun p -> p.Name = providedParam.name)
                            match matchingColumnType with
                            | None -> ()
                            | Some requiredParam ->
                                let typeMismatch (shouldUse: string list) =
                                    let formattedSuggestions =
                                        match shouldUse with
                                        | [ ] -> "<empty>"
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
                                      sprintf "Attempting to provide parameter '%s' of type '%s' using function %s. Please use %s instead."
                                        providedParam.name
                                        (requiredParam.EntityProperty.ToString())
                                        providedParam.paramFunc
                                        formattedSuggestions

                                    let codeFixs : Fix list =
                                        shouldUse
                                        |> List.map (fun func ->
                                            {
                                                FromText = providedParam.name
                                                FromRange = providedParam.paramFuncRange
                                                ToText = func
                                            })

                                    { createWarning warning providedParam.paramFuncRange with Fixes = codeFixs }

                                if providedParam.paramFunc = "AzureTable.parameter" then
                                    // do not do anything when the input is a generic param
                                    ()
                                else
                                    let entity =requiredParam.EntityProperty.ToString()
                                    match entity with
                                    | "bit" ->
                                        if requiredParam.IsNullable then
                                            if providedParam.paramFunc <> "AzureTable.bit" &&  providedParam.paramFunc <> "AzureTable.bitOrNone" && providedParam.paramFunc <> "AzureTable.dbnull"
                                            then yield typeMismatch [ "AzureTable.bit"; "AzureTable.bitOrNone";"AzureTable.dbnull"]
                                        else
                                            if providedParam.paramFunc <> "AzureTable.bit"
                                            then yield typeMismatch [ "AzureTable.bit" ]

                                    | ("bool" | "boolean") ->
                                        if requiredParam.IsNullable then
                                            if providedParam.paramFunc <> "AzureTable.bool" &&  providedParam.paramFunc <> "AzureTable.boolOrNone" && providedParam.paramFunc <> "AzureTable.dbnull"
                                            then yield typeMismatch [ "AzureTable.bool"; "AzureTable.boolOrNone"; "AzureTable.dbnull" ]
                                        else
                                            if providedParam.paramFunc <> "AzureTable.bool"
                                            then yield typeMismatch [ "AzureTable.bool" ]

                                    | ("int" | "int32" | "integer" | "serial") ->
                                        if requiredParam.IsNullable then
                                            if providedParam.paramFunc <> "AzureTable.int" &&  providedParam.paramFunc <> "AzureTable.intOrNone" && providedParam.paramFunc <> "AzureTable.dbnull"
                                            then yield typeMismatch [ "AzureTable.int"; "AzureTable.intOrNone"; "AzureTable.dbnull" ]
                                        else
                                            if providedParam.paramFunc <> "AzureTable.int"
                                            then yield typeMismatch [ "AzureTable.int" ]

                                    | ("smallint" | "int16") ->
                                        if requiredParam.IsNullable then
                                            if providedParam.paramFunc <> "AzureTable.int16" &&  providedParam.paramFunc <> "AzureTable.int16OrNone" && providedParam.paramFunc <> "AzureTable.dbnull"
                                            then yield typeMismatch [ "AzureTable.int16"; "AzureTable.int16OrNone"; "AzureTable.dbnull" ]
                                        else
                                            if providedParam.paramFunc <> "AzureTable.int16"
                                            then yield typeMismatch [ "AzureTable.int16" ]

                                    | ("int64" | "bigint" |"bigserial") ->
                                        if requiredParam.IsNullable then
                                            if providedParam.paramFunc <> "AzureTable.int64" && providedParam.paramFunc <> "AzureTable.int" &&  providedParam.paramFunc <> "AzureTable.int64OrNone" && providedParam.paramFunc <> "AzureTable.dbnull"
                                            then yield typeMismatch [ "AzureTable.int64"; "AzureTable.int"; "AzureTable.int64OrNone"; "AzureTable.dbnull" ]
                                        else
                                            if providedParam.paramFunc <> "AzureTable.int64" && providedParam.paramFunc <> "AzureTable.int"
                                            then yield typeMismatch [ "AzureTable.int64"; "AzureTable.int" ]

                                    | ("numeric" | "decimal" | "money") ->
                                        if requiredParam.IsNullable then
                                            if providedParam.paramFunc <> "AzureTable.decimal" &&  providedParam.paramFunc <> "AzureTable.decimalOrNone" && providedParam.paramFunc <> "AzureTable.dbnull"
                                            then yield typeMismatch [ "AzureTable.decimal"; "AzureTable.decimalOrNone"; "AzureTable.dbnull" ]
                                        else
                                            if providedParam.paramFunc <> "AzureTable.decimal"
                                            then yield typeMismatch [ "AzureTable.decimal" ]

                                    | "double precision" ->
                                        if requiredParam.IsNullable then
                                            if providedParam.paramFunc <> "AzureTable.double" &&  providedParam.paramFunc <> "AzureTable.doubleOrNone" && providedParam.paramFunc <> "AzureTable.dbnull"
                                            then yield typeMismatch [ "AzureTable.double"; "AzureTable.doubleOrNone"; "AzureTable.dbnull" ]
                                        else
                                            if providedParam.paramFunc <> "AzureTable.double"
                                            then yield typeMismatch [ "AzureTable.double" ]

                                    | "bytea" ->
                                        if requiredParam.IsNullable then
                                            if providedParam.paramFunc <> "AzureTable.bytea" &&  providedParam.paramFunc <> "AzureTable.byteaOrNone" && providedParam.paramFunc <> "AzureTable.dbnull"
                                            then yield typeMismatch [ "AzureTable.bytea"; "AzureTable.byteaOrNone"; "AzureTable.dbnull" ]
                                        else
                                            if providedParam.paramFunc <> "AzureTable.bytea"
                                            then yield typeMismatch [ "AzureTable.bytea" ]

                                    | "uuid" ->
                                        if requiredParam.IsNullable then
                                            if providedParam.paramFunc <> "AzureTable.uuid" &&  providedParam.paramFunc <> "AzureTable.uuidOrNone" && providedParam.paramFunc <> "AzureTable.dbnull"
                                            then yield typeMismatch [ "AzureTable.uuid"; "AzureTable.uuidOrNone"; "AzureTable.dbnull" ]
                                        else
                                            if providedParam.paramFunc <> "AzureTable.uuid"
                                            then yield typeMismatch [ "AzureTable.uuid" ]

                                    | "date" ->
                                        if requiredParam.IsNullable then
                                            if providedParam.paramFunc <> "AzureTable.date" &&  providedParam.paramFunc <> "AzureTable.dateOrNone" && providedParam.paramFunc <> "AzureTable.dbnull"
                                            then yield typeMismatch [ "AzureTable.date"; "AzureTable.dateOrNone"; "AzureTable.dbnull" ]
                                        else
                                            if providedParam.paramFunc <> "AzureTable.date"
                                            then yield typeMismatch [ "AzureTable.date" ]

                                    | ("timestamp"|"timestamp without time zone") ->
                                        if requiredParam.IsNullable then
                                            if providedParam.paramFunc <> "AzureTable.timestamp" &&  providedParam.paramFunc <> "AzureTable.timestampOrNone" && providedParam.paramFunc <> "AzureTable.dbnull"
                                            then yield typeMismatch [ "AzureTable.timestamp"; "AzureTable.timestampOrNone"; "AzureTable.dbnull" ]
                                        else
                                            if providedParam.paramFunc <> "AzureTable.timestamp"
                                            then yield typeMismatch [ "AzureTable.timestamp" ]

                                    | ("timestamptz" | "timestamp with time zone") ->
                                        if requiredParam.IsNullable then
                                            if providedParam.paramFunc <> "AzureTable.timestamptz" &&  providedParam.paramFunc <> "AzureTable.timestamptzOrNone" && providedParam.paramFunc <> "AzureTable.dbnull"
                                            then yield typeMismatch [ "AzureTable.timestamptz"; "AzureTable.timestamptzOrNone"; "AzureTable.dbnull" ]
                                        else
                                            if providedParam.paramFunc <> "AzureTable.timestamptz"
                                            then yield typeMismatch [ "AzureTable.timestamptz" ]

                                    | "jsonb" ->
                                        if requiredParam.IsNullable then
                                            if providedParam.paramFunc <> "AzureTable.jsonb" && providedParam.paramFunc <> "AzureTable.jsonbOrNone" && providedParam.paramFunc <> "AzureTable.dbnull"
                                            then yield typeMismatch [ "AzureTable.jsonb"; "AzureTable.jsonbOrNone"; "AzureTable.dbnull" ]
                                        else
                                            if providedParam.paramFunc <> "AzureTable.jsonb"
                                            then yield typeMismatch [ "AzureTable.jsonb" ]


                                    | _ ->
                                        ()

                ]

    let findColumn (name: string) (availableColumns: InformationSchema.TableInfo list) =
        availableColumns
        |> List.tryFind (fun column -> column.ColumnName = name)

    let formatColumns (availableColumns: InformationSchema.TableInfo list) =
        availableColumns
        |> List.map (fun column ->
            sprintf "| -- %s of type %A" column.ColumnName column.EntityProperty
        )
        |> String.concat "\n"

    let analyzeColumnReadingAttempts (columnReadAttempts: ColumnReadAttempt list) (availableColumns: InformationSchema.TableInfo list) =
        [
            for attempt in columnReadAttempts do
                match findColumn attempt.columnName availableColumns with
                | None ->
                    if List.isEmpty availableColumns then
                        let warningMsg = sprintf "Attempting to read column named '%s' from a result set which doesn't return any columns. In case you are executing DELETE, INSERT or UPDATE queries, you might want to use AzureTable.executeNonQuery or AzureTable.executeNonQueryAsync to obtain the number of affected rows. You can also add a RETURNING clause in your query to make it return the rows which are updated, inserted or deleted from that query." attempt.columnName
                        yield createWarning warningMsg attempt.columnNameRange
                    else
                    let levenshtein = NormalizedLevenshtein()
                    let closestAlternative =
                        availableColumns
                        |> List.minBy (fun column -> levenshtein.Distance(attempt.columnName, column.ColumnName))
                        |> fun column -> column.ColumnName

                    let warningMsg =
                        if String.IsNullOrWhiteSpace attempt.columnName then
                            sprintf "Attempting to read a column without specifying a name. Available columns returned from the result set are:\n%s" (formatColumns availableColumns)
                        else
                            sprintf "Attempting to read column named '%s' that was not returned by the result set. Did you mean to read '%s'?\nAvailable columns from the result set are:\n%s" attempt.columnName closestAlternative (formatColumns availableColumns)

                    let warning = createWarning warningMsg attempt.columnNameRange
                    let codeFixes =
                        availableColumns
                        |> List.map (fun column ->
                            { FromRange = attempt.columnNameRange
                              FromText = attempt.columnName
                              ToText = sprintf "\"%s\"" column.ColumnName })

                    yield { warning with Fixes = codeFixes }

                | Some column ->
                    let typeMismatchMessage (shouldUse: string list) =
                        let formattedFunctions =
                            match shouldUse with
                            | [ ] -> "<empty>"
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

                        String.concat String.Empty [
                            sprintf "Type mismatch: attempting to read column '%s' of " column.ColumnName
                            sprintf "type '%s' using %s. " (column.EntityProperty.ToString()) attempt.funcName
                            sprintf "Please use %s instead" formattedFunctions
                        ]

                    let typeMismatch (shouldUse: string list) =
                        let warningMessage = typeMismatchMessage shouldUse
                        let fixes =
                            shouldUse
                            |> List.map (fun func -> {
                                FromRange = attempt.funcCallRange
                                FromText = attempt.funcName
                                ToText = func
                            })

                        { createWarning warningMessage attempt.funcCallRange with Fixes = fixes }

                    if attempt.funcName.StartsWith "AzureTable." then
                        match column.EntityProperty.ToString() with
                        | ("bit"|"bool"|"boolean") when attempt.funcName <> "AzureTable.readBool" ->
                            yield typeMismatch [ "AzureTable.readBool" ]
                        | ("character varying"|"character"|"char"|"varchar"|"citext") when attempt.funcName <> "AzureTable.readString" ->
                            yield typeMismatch [ "AzureTable.readString" ]
                        | ("int" | "int2" | "int4" | "smallint" | "integer") when attempt.funcName <> "AzureTable.readInt" ->
                            yield typeMismatch [ "AzureTable.readInt" ]
                        | ("int8" | "bigint") when attempt.funcName <> "AzureTable.readLong" ->
                            yield typeMismatch [ "AzureTable.readLong" ]
                        | ("real" | "float4" | "double precision" | "float8") when attempt.funcName <> "AzureTable.readNumber" ->
                            yield typeMismatch ["AzureTable.readNumber"]
                        | ("numeric" | "decimal" | "money") when attempt.funcName <> "AzureTable.readDecimal" || attempt.funcName <> "AzureTable.readMoney" ->
                            yield typeMismatch ["AzureTable.readDecimal"]
                        | "bytea" when attempt.funcName <> "AzureTable.readBytea" ->
                            yield typeMismatch ["AzureTable.readBytea"]
                        | "uuid" when attempt.funcName <> "AzureTable.readUuid" ->
                            yield typeMismatch [ "AzureTable.readUuid" ]
                        | "date" when attempt.funcName <> "AzureTable.readDate" ->
                            yield typeMismatch [ "AzureTable.readDate" ]
                        | _ ->
                            ()
                    else
                        let replace (name: string) =
                            match attempt.funcName.Split '.' with
                            | [| reader; funcName |] -> sprintf "%s.%s" reader name
                            | _ -> attempt.funcName

                        let using (name: string) = attempt.funcName.EndsWith (sprintf ".%s" name)
                        let notUsing = using >> not

                        match column.EntityProperty.ToString() with
                        ("bit"|"bool"|"boolean") ->
                            if column.Nullable && notUsing "boolOrNone"
                            then yield typeMismatch [ replace "boolOrNone" ]
                            else if notUsing "boolOrNone" && notUsing "bool"
                            then
                                if column.Nullable
                                then yield typeMismatch [ replace "boolOrNone" ]
                                else yield typeMismatch [ replace "bool" ]
                            else ()

                        | ("int8" | "tinyint") ->
                            if column.Nullable && List.forall notUsing [ "int8OrNone"; "int16OrNone"; "intOrNone"; "int64OrNone" ]
                            then yield typeMismatch [ replace "int8OrNone"; replace "int16OrNone"; replace "intOrNone"; replace "int64OrNone" ]
                            else if List.forall notUsing [ "int8OrNone"; "int16OrNone"; "intOrNone"; "int64OrNone"; "int8"; "int16"; "int"; "int64" ]
                            then
                                if column.Nullable
                                then yield typeMismatch [ replace "int8OrNone"; replace "int16OrNone"; replace "intOrNone"; replace "int64OrNone" ]
                                else yield typeMismatch [ replace "int8"; replace "int16"; replace "int"; replace "int64" ]

                        | ("int16"| "smallint") ->
                            if column.Nullable && List.forall notUsing [ "int16OrNone"; "intOrNone"; "int64OrNone" ]
                            then yield typeMismatch [ replace "int16OrNone"; replace "intOrNone"; replace "int64OrNone" ]
                            else if List.forall notUsing [ "int16OrNone"; "intOrNone"; "int64OrNone"; "int16"; "int"; "int64" ]
                            then
                                if column.Nullable
                                then yield typeMismatch [ replace "int16OrNone"; replace "intOrNone"; replace "int64OrNone" ]
                                else yield typeMismatch [ replace "int16"; replace "int"; replace "int64" ]

                        | ("int"|"integer"|"int32"|"serial"|"int4"|"int2") ->
                            if column.Nullable && List.forall notUsing [ "intOrNone"; "int64OrNone" ]
                            then yield typeMismatch [ replace "intOrNone"; replace "int64OrNone" ]
                            else if List.forall notUsing [ "intOrNone"; "int64OrNone"; "int"; "int64" ]
                            then
                                if column.Nullable
                                then yield typeMismatch [ replace "intOrNone"; replace "int64OrNone" ]
                                else yield typeMismatch [ replace "int"; replace "int64" ]

                        | ("int64"|"bigint"|"bigserial") ->
                            if column.Nullable && notUsing "int64OrNone"
                            then yield typeMismatch [ replace "int64OrNone" ]
                            else if notUsing "int64OrNone" && notUsing "int64"
                            then
                                if column.Nullable
                                then yield typeMismatch [ replace "int64OrNone" ]
                                else yield typeMismatch [ replace "int64" ]
                            else ()

                        | ("numeric"|"decimal"|"money") ->
                            if column.Nullable && notUsing "decimalOrNone"
                            then yield typeMismatch [ replace "decimalOrNone" ]
                            else if notUsing "decimalOrNone" && notUsing "decimal"
                            then
                                if column.Nullable
                                then yield typeMismatch [ replace "decimalOrNone" ]
                                else yield typeMismatch [ replace "decimal" ]
                            else ()

                        | "double precision" ->
                            if column.Nullable && notUsing "doubleOrNone"
                            then yield typeMismatch [ replace "doubleOrNone" ]
                            else if notUsing "doubleOrNone" && notUsing "double"
                            then
                                if column.Nullable
                                then yield typeMismatch [ replace "doubleOrNone" ]
                                else yield typeMismatch [ replace "double" ]
                            else ()

                        | ("text"|"json"|"xml"|"jsonb"|"varchar") ->
                            if column.Nullable && notUsing "textOrNone" && notUsing "stringOrNone"
                            then yield typeMismatch [ replace "textOrNone"; replace "stringOrNone" ]
                            else if notUsing "textOrNone" && notUsing "text" && notUsing "string" && notUsing "stringOrNone"
                            then
                                if column.Nullable
                                then yield typeMismatch [ replace "textOrNone"; replace "stringOrText" ]
                                else yield typeMismatch [ replace "text"; replace "string" ]
                            else ()

                        | "date" ->
                            if column.Nullable && notUsing "dateOrNone"
                            then yield typeMismatch [ replace "dateOrNone" ]
                            else if notUsing "dateOrNone" && notUsing "date"
                            then
                                if column.Nullable
                                then yield typeMismatch [ replace "dateOrNone" ]
                                else yield typeMismatch [ replace "date" ]
                            else ()

                        | ("timestamp"|"timestamp without time zone") ->
                            if column.Nullable && notUsing "timestampOrNone" && notUsing "dateTimeOrNone"
                            then yield typeMismatch [ replace "dateTimeOrNone"; replace "timestampOrNone" ]
                            else if notUsing "timestampOrNone" && notUsing "timestamp" && notUsing "dateTimeOrNone" && notUsing "dateTime"
                            then
                                if column.Nullable
                                then yield typeMismatch [ replace "dateTimeOrNone"; replace "timestampOrNone" ]
                                else yield typeMismatch [ replace "dateTime"; replace "timestamp" ]
                            else ()

                        | ("timestamptz"|"timestamp with time zone") ->
                            if column.Nullable && notUsing "timestamptzOrNone" && notUsing "dateTimeOrNone" && notUsing "datetimeOffsetOrNone"
                            then yield typeMismatch [ replace "datetimeOffsetOrNone"; replace "dateTimeOrNone"; replace "timestamptzOrNone" ]
                            //else if not column.Nullable && (using "timestamptzOrNone" || using "dateTimeOrNone")
                            //then yield typeMismatch [ replace "dateTime"; replace "timestamptz" ]
                            else if notUsing "timestamptzOrNone" && notUsing "timestamptz" && notUsing "dateTimeOrNone" && notUsing "dateTime" && notUsing "datetimeOffsetOrNone" && notUsing "datetimeOffset"
                            then
                                if column.Nullable
                                then yield typeMismatch [ replace "datetimeOffsetOrNone"; replace "dateTimeOrNone"; replace "timestamptzOrNone" ]
                                else yield typeMismatch [ replace "datetimeOffset"; replace "dateTime"; replace "timestamptz" ]
                            else ()

                        | "bytea" ->
                            if column.Nullable && notUsing "byteaOrNone"
                            then yield typeMismatch [ replace "byteaOrNone" ]
                            else if notUsing "byteaOrNone" && notUsing "bytea"
                            then
                                if column.Nullable
                                then yield typeMismatch [ replace "byteaOrNone" ]
                                else yield typeMismatch [ replace "bytea" ]
                            else ()
                        | _ ->
                                ()
        ]

    /// Tries to read the database schema from the connection string
    let databaseSchema (connectionString:string) =
        task {
            try
                let! tables = getAzureTables(connectionString)
                return Ok (tables)
            with | ex -> return Result.Error ("Can't get tables exn: " + ex.Message)
        }
    /// Tries to read the database schema from the connection string
    let entities connectionString tableName =
        task {
            try
                let! entities = getAzureTableEntity(connectionString,tableName)
                return Ok (entities)
            with | ex -> return Result.Error ex.Message
        }

    /// Uses database schema that is retrieved once during initialization
    /// and re-used when analyzing the rest of the Sql operation blocks
    let analyzeOperation (operation: AzureOperation) (connectionString: string) (tableList: InformationSchema.TableList) =
        task {
            match findTable operation with
            | None ->
                    return []
            | Some (table, tableRange) ->
                let! queryAnalysis = extractFiltersAndOutputColumns(connectionString, table)
                match queryAnalysis with
                | Result.Error queryError ->
                    return [ createWarning queryError tableRange ]
                | Result.Ok tableInfos ->

                    let readingAttempts = defaultArg (findColumnReadAttempts operation) [ ]
                    let readingAttempts = defaultArg (findColumnReadAttempts operation) [ ]
                    return [
                        // createInfo (tableInfos.ToString()) tableRange

                        // yield! analyzeFilter (findFilters operation) operation.range tableInfos
                        yield! analyzeColumnReadingAttempts readingAttempts tableInfos
                    ]
        }
        |> Async.AwaitTask
        |> Async.RunSynchronously
