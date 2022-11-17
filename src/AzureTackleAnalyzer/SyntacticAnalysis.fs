namespace AzureTackle.Analyzers.Core

open FSharp.Compiler.EditorServices
open FSharp.Compiler.Text
open FSharp.Compiler.Symbols
open FSharp.Compiler.Syntax

module SyntacticAnalysis =
  let (|FuncName|_|) =
    function
    | SynExpr.Ident ident -> Some(ident.idText)
    | SynExpr.LongIdent(isOptional, longDotId, altName, range) ->
      match longDotId with
      | LongIdentWithDots(listOfIds, ranges) ->
        let fullName = listOfIds |> List.map (fun id -> id.idText) |> String.concat "."

        Some fullName
    | _ -> None

  let (|Apply|_|) =
    function
    | SynExpr.App(atomicFlag, isInfix, funcExpr, argExpr, applicationRange) ->
      match funcExpr with
      | SynExpr.Ident ident -> Some(ident.idText, argExpr, funcExpr.Range, applicationRange)
      | SynExpr.LongIdent(isOptional, longDotId, altName, identRange) ->
        match longDotId with
        | LongIdentWithDots(listOfIds, ranges) ->
          let fullName = listOfIds |> List.map (fun id -> id.idText) |> String.concat "."

          Some(fullName, argExpr, funcExpr.Range, applicationRange)
      | _ -> None
    | _ -> None

  let (|Applied|_|) =
    function
    | SynExpr.App(atomicFlag, isInfix, funcExpr, argExpr, applicationRange) ->
      match argExpr with
      | SynExpr.Ident ident -> Some(ident.idText, funcExpr.Range, applicationRange)
      | SynExpr.LongIdent(isOptional, longDotId, altName, identRange) ->
        match longDotId with
        | LongIdentWithDots(listOfIds, ranges) ->
          let fullName = listOfIds |> List.map (fun id -> id.idText) |> String.concat "."

          Some(fullName, funcExpr.Range, applicationRange)
      | _ -> None
    | _ -> None

  let (|FilterTuple|_|) =
    function
    | SynExpr.Tuple(isStruct,
                    [ SynExpr.Const(SynConst.String(filterName, SynStringKind.Regular, paramRange), constRange)
                      Apply(funcName, exprArgs, funcRange, appRange) ],
                    commaRange,
                    tupleRange) ->

      Some(filterName, paramRange, funcName, funcRange, Some appRange)
    | SynExpr.Tuple(isStruct,
                    [ SynExpr.Ident ident
                      SynExpr.Const(SynConst.String(filterName, SynStringKind.Regular, paramRange), constRange) ],
                    commaRange,
                    tupleRange) -> Some(filterName, paramRange, ident.ToString(), constRange, Some constRange)
    | _ -> None
    | _ -> None

  let rec readfilters =
    function
    | FilterTuple(name, range, func, funcRange, appRange) -> [ name, range, func, funcRange, appRange ]
    | SynExpr.Sequential(_debugSeqPoint, isTrueSeq, expr1, expr2, seqRange) ->
      [ yield! readfilters expr1; yield! readfilters expr2 ]
    | SynExpr.App(flag, isInfix, funcExpr, argExpr, range) ->
      [ yield! readfilters funcExpr; yield! readfilters argExpr ]
    | SynExpr.Ident(x) -> [ x.idText, x.idRange, "Ident", x.idRange, None ]
    | SynExpr.Const(c, r) -> [ c.ToString(), r, "Const", r, None ]
    | SynExpr.Paren(expr, leftParRange, rightParRange, range) -> [ yield! readfilters expr ]

    | x ->
      printfn "unmatched SynExpr %A" x
      []


  let rec flattenList =
    function
    | SynExpr.Sequential(_debugSeqPoint, isTrueSeq, expr1, expr2, seqRange) ->
      [ yield! flattenList expr1; yield! flattenList expr2 ]
    | expr -> [ expr ]

  /// Detects `AzureTable.filter {FilterArray}` pattern
  let (|AzureFilters|_|) =
    function
    | Apply("AzureTable.filter", SynExpr.ArrayOrListComputed(isArray, listExpr, listRange), funcRange, appRange) ->
      match listExpr with
      | SynExpr.ComputationExpr(hasSeqBuilder, compExpr, compRange) -> Some(readfilters compExpr, compRange)
      | _ -> None
    | _ -> None

  let readFilterSets filterSetsExpr =
    let sets = ResizeArray<FilterSet>()

    match filterSetsExpr with
    | SynExpr.ArrayOrListComputed(isArray, listExpr, listRange) ->
      match listExpr with
      | SynExpr.ComputationExpr(hasSeqBuilder, outerListExpr, outerListRange) ->
        match outerListExpr with
        | SynExpr.ForEach(_, _, _, _,_, enumExpr, bodyExpr, forEachRange) ->
          match bodyExpr with
          | SynExpr.YieldOrReturn(_, outputExpr, outputExprRange) ->
            match outputExpr with
            | SynExpr.ArrayOrListComputed(isArray, filterListExpr, filterListRange) ->
              let filterSet =
                { range = filterListRange
                  filters = [] }

              match filterListExpr with
              | SynExpr.ComputationExpr(hasSeqBuilder, compExpr, compRange) ->
                let filters: UsedFilter list =
                  [ for expr in flattenList compExpr do
                      match expr with
                      | FilterTuple(name, range, func, funcRange, appRange) ->
                        { filterFunc = func
                          filterFuncRange = funcRange
                          name = name.TrimStart '@'
                          range = range
                          applicationRange = appRange }
                      | _ -> () ]

                sets.Add { filterSet with filters = filters }

              | _ -> ()

            | SynExpr.ArrayOrList(isList, expressions, range) ->
              let filters: UsedFilter list =
                [ for expr in expressions do
                    match expr with
                    | FilterTuple(name, range, func, funcRange, appRange) ->
                      { filterFunc = func
                        filterFuncRange = funcRange
                        name = name.TrimStart '@'
                        range = range
                        applicationRange = appRange }
                    | _ -> () ]

              sets.Add { range = range; filters = filters }

            | _ -> ()
          | _ -> ()
        | _ ->
          let filterSets = flattenList outerListExpr

          for filterSetExpr in filterSets do
            match filterSetExpr with
            | SynExpr.ArrayOrListComputed(isArray, filterListExpr, filterListRange) ->
              let filterSet =
                { range = filterListRange
                  filters = [] }

              match filterListExpr with
              | SynExpr.ComputationExpr(hasSeqBuilder, compExpr, compRange) ->
                let filters: UsedFilter list =
                  [ for expr in flattenList compExpr do
                      match expr with
                      | FilterTuple(name, range, func, funcRange, appRange) ->
                        { filterFunc = func
                          filterFuncRange = funcRange
                          name = name.TrimStart '@'
                          range = range
                          applicationRange = appRange }
                      | _ -> () ]

                sets.Add { filterSet with filters = filters }

              | _ -> ()

            | SynExpr.ArrayOrList(isList, expressions, range) ->
              let filters: UsedFilter list =
                [ for expr in expressions do
                    match expr with
                    | FilterTuple(name, range, func, funcRange, appRange) ->
                      { filterFunc = func
                        filterFuncRange = funcRange
                        name = name.TrimStart '@'
                        range = range
                        applicationRange = appRange }
                    | _ -> () ]

              sets.Add { range = range; filters = filters }

            | _ -> ()
      | _ -> ()

    | _ -> ()

    Seq.toList sets

  let (|ReadColumnAttempt|_|) =
    function
    | Apply(funcName,
            SynExpr.Const(SynConst.String(columnName, SynStringKind.Regular, queryRange), constRange),
            funcRange,
            appRange) ->
      if funcName.StartsWith "AzureTable.read" && funcName <> "AzureTable.readRow" then
        Some
          { funcName = funcName
            columnName = columnName
            columnNameRange = constRange
            funcCallRange = funcRange }
      else
        let possibleFunctions =
          [ ".int"
            ".intOrNone"
            ".bool"
            ".bootOrNone"
            ".text"
            ".textOrNone"
            ".int16"
            ".int16OrNone"
            ".int64"
            ".int64OrNone"
            ".string"
            ".stringorNone"
            ".decimal"
            ".decimalOrNone"
            ".bytea"
            ".byteaOrNone"
            ".double"
            ".doubleOrNone"
            ".timestamp"
            ".timestampOrNone"
            ".timestamptz"
            ".timestamptzOrNone"
            ".uuid"
            ".uuidOrNone"
            ".float"
            ".floatOrNone"
            ".interval"
            ".date"
            ".dateOrNone"
            ".dateTime"
            ".dateTimeOrNone"
            ".datetimeOffset"
            ".datetimeOffsetOrNone"
            ".intArray"
            ".intArrayOrNone"
            ".stringArray"
            ".stringArrayOrNone"
            ".uuidArray"
            ".uuidArrayOrNone" ]

        if possibleFunctions |> List.exists funcName.EndsWith then
          Some
            { funcName = funcName
              columnName = columnName
              columnNameRange = constRange
              funcCallRange = funcRange }
        else
          None
    | _ -> None

  /// Detects `AzureTable.table {tableName}` pattern
  let (|AzureTable|_|) =
    function
    | Apply("AzureTable.table",
            SynExpr.Const(SynConst.String(table, SynStringKind.Regular, tableRange), constRange),
            range,
            appRange) -> Some(table, constRange)
    | _ -> None

  let rec findTable =
    function
    | AzureTable(table, range) -> [ AzureAnalyzerBlock.Table(table, range) ]
    | SynExpr.App(exprAtomic, isInfix, funcExpr, argExpr, range) ->
      [ yield! findTable funcExpr; yield! findTable argExpr ]
    | _ -> []

  let rec findFunc =
    function
    | SynExpr.App(exprAtomic, isInfix, funcExpr, argExpr, range) ->
      [ yield! findFunc funcExpr; yield! findFunc argExpr ]
    | _ -> []

  let rec findFilters =
    function
    | AzureFilters(filters, range) ->
      let filters =
        filters
        |> List.map (fun (name, range, func, funcRange, appRange) ->
          printfn "filters %s" name

          { name = name.Trim().TrimStart('@')
            range = range
            filterFunc = func
            filterFuncRange = funcRange
            applicationRange = appRange })

      [ AzureAnalyzerBlock.Filters(filters, range) ]

    | SynExpr.App(exprAtomic, isInfix, funcExpr, argExpr, range) ->
      [ yield! findFilters funcExpr; yield! findFilters argExpr ]

    | _ -> []

  let rec findReadColumnAttempts =
    function
    | ReadColumnAttempt(attempt) -> [ attempt ]
    | SynExpr.App(exprAtomic, isInfix, funcExpr, argExpr, range) ->
      [ yield! findReadColumnAttempts funcExpr
        yield! findReadColumnAttempts argExpr ]
    | SynExpr.Paren(expr, leftRange, rightRange, range) -> [ yield! findReadColumnAttempts expr ]
    | SynExpr.Lambda(fromMethod, inLambdaSeq, args, body, _, range, trivia) -> [ yield! findReadColumnAttempts body ]
    | SynExpr.LetOrUse(isRecursive, isUse, bindings, body, range, trivia) ->
      [ yield! findReadColumnAttempts body
        for binding in bindings do
          match binding with
          | SynBinding (access,
                               kind,
                               mustInline,
                               isMutable,
                               attrs,
                               xmlDecl,
                               valData,
                               headPat,
                               returnInfo,
                               expr,
                               range,
                               seqPoint,travia) -> yield! findReadColumnAttempts expr ]

    | SynExpr.LetOrUseBang(sequencePoint, isUse, isFromSource, syntaxPattern, expr1, andExprs, body, range, trivia) ->
      [ yield! findReadColumnAttempts expr1
        yield! findReadColumnAttempts body
        for andExpr in andExprs do
          match andExpr with
          | SynExprAndBang   (pointInfo, _, _, pattern, expr, range,trivia) ->
            yield! findReadColumnAttempts expr ]

    | SynExpr.ComputationExpr( _, expression, range) -> [ yield! findReadColumnAttempts expression ]

    | SynExpr.AnonRecd(isStruct, copyInfo, recordFields, range) ->
      [ match copyInfo with
        | Some(expr, info) -> yield! findReadColumnAttempts expr
        | None -> ()

        for (_,_, expr) in recordFields do
          yield! findReadColumnAttempts expr ]

    | SynExpr.ArrayOrList(isList, elements, range) ->
      [ for elementExpr in elements do
          yield! findReadColumnAttempts elementExpr ]

    | SynExpr.ArrayOrListComputed(isArray, exp, range) -> [ yield! findReadColumnAttempts exp ]

    | SynExpr.Record(_, _, recordFields, _) ->
      [ for recordField in recordFields do
          match recordField with
          | SynExprRecordField (_,_,expr,_) ->

            match expr with
            | Some e -> yield! findReadColumnAttempts e
            | None -> () ]

    | SynExpr.IfThenElse(ifExpr, elseExpr, thenExpr, _, _, _, _) ->
      [ yield! findReadColumnAttempts ifExpr
        yield! findReadColumnAttempts elseExpr
        match thenExpr with
        | Some expr -> yield! findReadColumnAttempts expr
        | None -> () ]

    | SynExpr.Lazy(body, range) -> [ yield! findReadColumnAttempts body ]

    | SynExpr.New(protocol, typeName, expr, range) -> [ yield! findReadColumnAttempts expr ]

    | SynExpr.Tuple(isStruct, exprs, commaRanges, range) ->
      [ for expr in exprs do
          yield! findReadColumnAttempts expr ]

    | SynExpr.Match(seqPoint, matchExpr, clauses, range,travia) ->
      [ yield! findReadColumnAttempts matchExpr
        for clause in clauses do
            match clause with
            | SynMatchClause (pattern, whenExpr, body, range, seqPoint,travia) ->
                yield! findReadColumnAttempts body

                match whenExpr with
                | Some body -> yield! findReadColumnAttempts body
                | None -> () ]

    | SynExpr.MatchBang(seqPoint, matchExpr, clauses, range,travia) ->
      [ yield! findReadColumnAttempts matchExpr
        for clause in clauses do
            match clause with
            | SynMatchClause  (pattern, whenExpr, body, range, seqPoint,travia) ->
                yield! findReadColumnAttempts body

                match whenExpr with
                | Some body -> yield! findReadColumnAttempts body
                | None -> () ]

    | _ -> []


  let rec visitSyntacticExpression (expr: SynExpr) (fullExpressionRange: range) =
    match expr with
    | SynExpr.ComputationExpr(_, innerExpr, range) -> visitSyntacticExpression innerExpr range
    | SynExpr.YieldOrReturn(_, innerExpr, innerRange) -> visitSyntacticExpression innerExpr innerRange
    | SynExpr.YieldOrReturnFrom(_, innerExpr, innerRange) -> visitSyntacticExpression innerExpr innerRange
    | SynExpr.App(exprAtomic, isInfix, funcExpr, argExpr, range) ->
      match argExpr with
      | SynExpr.ComputationExpr(_, innerExpr, range) -> visitSyntacticExpression innerExpr range

      | Apply(("AzureTable.execute" | "AzureTable.executeDirect"), lambdaExpr, funcRange, appRange) ->
        let columns = findReadColumnAttempts lambdaExpr

        let blocks =
          [ yield! findTable funcExpr
            yield! findFilters funcExpr
            yield! findFunc funcExpr
            yield AzureAnalyzerBlock.ReadingColumns columns ]

        [ { blocks = blocks; range = range } ]

      | AzureTable(table, tableRange) ->
        printfn "tables %A" table

        let blocks =
          [ yield! findTable funcExpr; AzureAnalyzerBlock.Table(table, tableRange) ]

        [ { blocks = blocks; range = range } ]

      | AzureFilters(filters, range) ->
        printfn "filters %A" filters

        let azureFilters =
          filters
          |> List.map (fun (name, range, func, funcRange, appRange) ->
            { name = name.Trim().TrimStart('@')
              range = range
              filterFunc = func
              filterFuncRange = funcRange
              applicationRange = appRange })

        let blocks =
          [ yield! findFilters funcExpr
            yield AzureAnalyzerBlock.Filters(azureFilters, range) ]

        [ { blocks = blocks; range = range } ]

      | FuncName(functionWithoutfilters) ->
        let blocks = [ yield! findFunc funcExpr; yield! findFilters funcExpr ]

        [ { blocks = blocks; range = range } ]
      | Apply(anyOtherFunction, functionArg, range, appRange) ->
        let blocks =
          [ yield! findFunc funcExpr
            yield! findFilters funcExpr
            yield AzureAnalyzerBlock.ReadingColumns(findReadColumnAttempts funcExpr) ]

        [ { blocks = blocks; range = range } ]
      | SynExpr.Paren(innerExpr, leftRange, rightRange, range) -> visitSyntacticExpression innerExpr range
      | x ->
        printfn "unmatched expression %A" x
        []
    | SynExpr.LetOrUse(isRecursive, isUse, bindings, body, range,travia) ->
      [ yield! visitSyntacticExpression body range
        for binding in bindings do
          yield! visitBinding binding ]

    | SynExpr.App(flag, _, SynExpr.Ident ident, SynExpr.ComputationExpr( _, innerExpr, innerExprRange), r) when ident.idText = "async" ->
        visitSyntacticExpression innerExpr innerExprRange

    | SynExpr.LetOrUseBang(_, isUse, isFromSource, pat, rhs, andBangs, body, range,travia) ->
      [ yield! visitSyntacticExpression body range
        yield! visitSyntacticExpression rhs range ]

    | SynExpr.IfThenElse(ifExpr, thenExpr, elseExpr, spIfToThen, isFromErrorRecovery, range, travia) ->
      [ yield! visitSyntacticExpression ifExpr range
        yield! visitSyntacticExpression thenExpr range
        match elseExpr with
        | None -> ()
        | Some expr -> yield! visitSyntacticExpression expr range ]

    | SynExpr.Lambda(fromMethod, inSeq, args, body, parsedData, range,travia) -> visitSyntacticExpression body range

    | SynExpr.Sequential(debugSeqPoint, isTrueSeq, expr1, expr2, range) ->
      [ yield! visitSyntacticExpression expr1 range
        yield! visitSyntacticExpression expr2 range ]

    | otherwise ->
      printfn "unmatched expression %A" otherwise
      []

  and visitBinding (binding: SynBinding) : AzureOperation list =
    match binding with
    | SynBinding(access,
                 kind,
                 mustInline,
                 isMutable,
                 attrs,
                 xmlDecl,
                 valData,
                 headPat,
                 returnInfo,
                 expr,
                 range,
                 seqPoint,
                 travia) -> visitSyntacticExpression expr range

  let findLiterals (ctx: AzureTableAnalyzerContext) =
    let values = new ResizeArray<string * string>()

    for symbol in ctx.Symbols |> Seq.collect (fun s -> s.TryGetMembersFunctionsAndValues()) do
      match symbol.LiteralValue with
      | Some value when value.GetType() = typeof<string> -> values.Add((symbol.LogicalName, unbox<string> value))
      | _ -> ()

    Map.ofSeq values

  /// Tries to replace [<Literal>] strings inside the module with the identifiers that were used with Sql.query.
  let applyLiterals (literals: Map<string, string>) (operation: AzureOperation) =
    let modifiedBlocks =
      operation.blocks
      |> List.choose (function
        | differentBlock -> Some differentBlock)

    { operation with blocks = modifiedBlocks }

  let findAzureOperations (ctx: AzureTableAnalyzerContext) =
    let operations = ResizeArray<AzureOperation>()

    match ctx.ParseTree with
    | ParsedInput.ImplFile input ->
      match input with
      | ParsedImplFileInput(fileName, isScript, qualifiedName, _, _, modules, _, trivia) ->
        for parsedModule in modules do
          match parsedModule with
          | SynModuleOrNamespace(identifier, isRecursive, kind, declarations, _, _, _, _, trivia) ->
            let rec iterTypeDefs (defs: list<SynTypeDefn>) =
              for def in defs do
                match def with
                | SynTypeDefn(typeInfo, typeRepr, members, implicitConstructor, range, travia) ->
                  for memberDefn in members do
                    match memberDefn with
                    | SynMemberDefn.Member(binding, _) -> operations.AddRange(visitBinding binding)
                    | SynMemberDefn.LetBindings(bindings, _, _, _) ->
                      for binding in bindings do
                        operations.AddRange(visitBinding binding)
                    | SynMemberDefn.NestedType(nestedTypeDef, _, _) -> iterTypeDefs [ nestedTypeDef ]
                    | x ->
                      printfn "unmatched member %A" x
                      ()

                  match typeRepr with
                  | SynTypeDefnRepr.ObjectModel(modelKind, members, range) ->
                    for memberDefn in members do
                      match memberDefn with
                      | SynMemberDefn.Member(binding, _) -> operations.AddRange(visitBinding binding)
                      | SynMemberDefn.LetBindings(bindings, _, _, _) ->
                        for binding in bindings do
                          operations.AddRange(visitBinding binding)
                      | SynMemberDefn.NestedType(nestedTypeDef, _, _) -> iterTypeDefs [ nestedTypeDef ]
                      | x ->
                        printfn "unmatched member %A" x
                        ()

            let rec iterDeclarations decls =
              for declaration in decls do
                match declaration with
                | SynModuleDecl.Let(isRecursiveDef, bindings, range) ->
                  for binding in bindings do
                    operations.AddRange(visitBinding binding)
                | SynModuleDecl.NestedModule(moduleInfo, isRecursive, nestedDeclarations, _, _, trivia) ->
                  iterDeclarations nestedDeclarations

                | SynModuleDecl.Types(definitions, range) -> iterTypeDefs definitions

                | SynModuleDecl.Expr(expression, range) ->
                  operations.AddRange(visitSyntacticExpression expression range)
                | x -> ()

            iterDeclarations declarations

    | ParsedInput.SigFile file -> ()

    let moduleLiterals = findLiterals ctx

    operations
    |> Seq.map (applyLiterals moduleLiterals)
    |> Seq.filter (fun operation -> not (List.isEmpty operation.blocks))
    |> Seq.toList
