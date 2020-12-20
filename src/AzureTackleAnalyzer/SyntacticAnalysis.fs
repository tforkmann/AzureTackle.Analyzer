namespace AzureTackle.Analyzers.Core

open FSharp.Compiler.SourceCodeServices
open FSharp.Compiler.SyntaxTree
open FSharp.Compiler.Range

module SyntacticAnalysis =
    let (|FuncName|_|) = function
        | SynExpr.Ident ident -> Some (ident.idText)
        | SynExpr.LongIdent(isOptional, longDotId, altName, range) ->
            match longDotId with
            | LongIdentWithDots(listOfIds, ranges) ->
                let fullName =
                    listOfIds
                    |> List.map (fun id -> id.idText)
                    |> String.concat "."

                Some fullName
        | _ -> None

    let (|Apply|_|) = function
        | SynExpr.App(atomicFlag, isInfix, funcExpr, argExpr, applicationRange) ->
            match funcExpr with
            | SynExpr.Ident ident -> Some (ident.idText, argExpr, funcExpr.Range, applicationRange)
            | SynExpr.LongIdent(isOptional, longDotId, altName, identRange) ->
                match longDotId with
                | LongIdentWithDots(listOfIds, ranges) ->
                    let fullName =
                        listOfIds
                        |> List.map (fun id -> id.idText)
                        |> String.concat "."

                    Some (fullName, argExpr, funcExpr.Range, applicationRange)
            | _ -> None
        | _ -> None

    let (|Applied|_|) = function
        | SynExpr.App(atomicFlag, isInfix, funcExpr, argExpr, applicationRange) ->
            match argExpr with
            | SynExpr.Ident ident -> Some (ident.idText, funcExpr.Range, applicationRange)
            | SynExpr.LongIdent(isOptional, longDotId, altName, identRange) ->
                match longDotId with
                | LongIdentWithDots(listOfIds, ranges) ->
                    let fullName =
                        listOfIds
                        |> List.map (fun id -> id.idText)
                        |> String.concat "."

                    Some (fullName, funcExpr.Range, applicationRange)
            | _ -> None
        | _ -> None

    let (|ParameterTuple|_|) = function
        | SynExpr.Tuple(isStruct, [ SynExpr.Const(SynConst.String(parameterName, paramRange), constRange); Apply(funcName, exprArgs, funcRange, appRange) ], commaRange, tupleRange) ->
            Some (parameterName, paramRange, funcName, funcRange, Some appRange)
        | SynExpr.Tuple(isStruct, [ SynExpr.Const(SynConst.String(parameterName, paramRange), constRange); secondItem ], commaRange, tupleRange) ->
            match secondItem with
            | SynExpr.LongIdent(isOptional, longDotId, altName, identRange) ->
                match longDotId with
                | LongIdentWithDots(listOfIds, ranges) ->
                    let fullName =
                        listOfIds
                        |> List.map (fun id -> id.idText)
                        |> String.concat "."

                    Some (parameterName, paramRange, fullName, identRange, None)
            | _ ->
                None
        | _ ->
            None

    let rec readParameters = function
        | ParameterTuple (name, range, func, funcRange, appRange) ->
            [ name, range, func, funcRange, appRange ]
        | SynExpr.Sequential(_debugSeqPoint, isTrueSeq, expr1, expr2, seqRange) ->
            [ yield! readParameters expr1; yield! readParameters expr2 ]
        | _ ->
            [ ]

    let rec flattenList = function
        | SynExpr.Sequential(_debugSeqPoint, isTrueSeq, expr1, expr2, seqRange) ->
            [ yield! flattenList expr1; yield! flattenList expr2 ]
        | expr ->
            [ expr ]

    let (|AzureFilters|_|) = function
        | Apply ("AzureTable.filter", SynExpr.ArrayOrListOfSeqExpr(isArray, listExpr, listRange) , funcRange, appRange) ->
            match listExpr with
            | SynExpr.CompExpr(isArrayOfList, isNotNakedRefCell, compExpr, compRange) ->
                Some (readParameters compExpr, compRange)
            | _ ->
                None
        | _ ->
            None

    let readFilterSets parameterSetsExpr =
        let sets = ResizeArray<FilterSet>()

        match parameterSetsExpr with
        | SynExpr.ArrayOrListOfSeqExpr(isArray, listExpr, listRange) ->
            match listExpr with
            | SynExpr.CompExpr(isArrayOfList, isNotNakedRefCell, outerListExpr, outerListRange) ->
                match outerListExpr with
                | SynExpr.ForEach(_, _, _, _, enumExpr, bodyExpr, forEachRange) ->
                    match bodyExpr with
                    | SynExpr.YieldOrReturn(_, outputExpr, outputExprRange) ->
                        match outputExpr with
                        | SynExpr.ArrayOrListOfSeqExpr(isArray, parameterListExpr, parameterListRange) ->
                            let parameterSet = {
                                range = parameterListRange
                                filters = [ ]
                            }

                            match parameterListExpr with
                            | SynExpr.CompExpr(isArrayOfList, isNotNakedRefCell, compExpr, compRange) ->
                                let filters : UsedFilter list = [
                                    for expr in flattenList compExpr do
                                        match expr with
                                        | ParameterTuple(name, range, func, funcRange, appRange) ->
                                            {
                                                paramFunc = func
                                                paramFuncRange = funcRange
                                                name = name.TrimStart '@'
                                                range = range
                                                applicationRange = appRange
                                            }
                                        | _ ->
                                            ()
                                ]

                                sets.Add { parameterSet with filters = filters }

                            | _ ->
                                ()

                        | SynExpr.ArrayOrList(isList, expressions, range) ->
                            let filters : UsedFilter list = [
                                for expr in expressions do
                                    match expr with
                                    | ParameterTuple(name, range, func, funcRange, appRange) ->
                                        {
                                            paramFunc = func
                                            paramFuncRange = funcRange
                                            name = name.TrimStart '@'
                                            range = range
                                            applicationRange = appRange
                                        }
                                    | _ ->
                                        ()
                            ]

                            sets.Add {
                                range = range
                                filters = filters
                            }

                        | _ ->
                            ()
                    | _ ->
                        ()
                | _ ->
                    let parameterSets = flattenList outerListExpr

                    for parameterSetExpr in parameterSets do
                        match parameterSetExpr with
                        | SynExpr.ArrayOrListOfSeqExpr(isArray, parameterListExpr, parameterListRange) ->
                            let parameterSet = {
                                range = parameterListRange
                                filters = [ ]
                            }

                            match parameterListExpr with
                            | SynExpr.CompExpr(isArrayOfList, isNotNakedRefCell, compExpr, compRange) ->
                                let filters : UsedFilter list = [
                                    for expr in flattenList compExpr do
                                        match expr with
                                        | ParameterTuple(name, range, func, funcRange, appRange) ->
                                            {
                                                paramFunc = func
                                                paramFuncRange = funcRange
                                                name = name.TrimStart '@'
                                                range = range
                                                applicationRange = appRange
                                            }
                                        | _ ->
                                            ()
                                ]

                                sets.Add { parameterSet with filters = filters }

                            | _ ->
                                ()

                        | SynExpr.ArrayOrList(isList, expressions, range) ->
                            let filters : UsedFilter list = [
                                for expr in expressions do
                                    match expr with
                                    | ParameterTuple(name, range, func, funcRange, appRange) ->
                                        {
                                            paramFunc = func
                                            paramFuncRange = funcRange
                                            name = name.TrimStart '@'
                                            range = range
                                            applicationRange = appRange
                                        }
                                    | _ ->
                                        ()
                            ]

                            sets.Add {
                                range = range
                                filters = filters
                            }

                        | _ ->
                            ()
            | _ ->
                ()

        | _ -> ()

        Seq.toList sets

    let (|TransactionQuery|_|) = function
        | SynExpr.Tuple(isStruct, [ SynExpr.Const(SynConst.String(query, queryRange), constRange); filterSetsExpr ], commaRange, tupleRange) ->
            let transaction =  {
                query = query;
                queryRange = queryRange
                filterSets = readFilterSets filterSetsExpr
            }

            Some transaction

        | SynExpr.Tuple(isStruct, [ SynExpr.Ident value; filterSetsExpr ], commaRange, tupleRange) ->
            let transaction =  {
                query = value.idText;
                queryRange = value.idRange
                filterSets = readFilterSets filterSetsExpr
            }

            Some transaction
        | _ ->
            None

    let rec readTransactionQueries = function
        | TransactionQuery transactionQuery ->
            [ transactionQuery ]
        | SynExpr.Sequential(_debugSeqPoint, isTrueSeq, expr1, expr2, seqRange) ->
            [
                yield! readTransactionQueries expr1;
                yield! readTransactionQueries expr2
            ]
        | _ ->
            [ ]

    let (|ReadColumnAttempt|_|) = function
        | Apply(funcName, SynExpr.Const(SynConst.String(columnName, queryRange), constRange), funcRange, appRange) ->
            if funcName.StartsWith "AzureTable.read" && funcName <> "AzureTable.readRow"
            then Some {
                funcName = funcName
                columnName = columnName
                columnNameRange = constRange
                funcCallRange = funcRange }
            else
                let possibleFunctions = [
                    ".int"
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
                    ".uuidArrayOrNone"
                ]

                if possibleFunctions |> List.exists funcName.EndsWith then
                    Some {
                        funcName = funcName
                        columnName = columnName
                        columnNameRange = constRange
                        funcCallRange = funcRange }
                else
                    None
        | _ ->
           None

    /// Detects `AzureTable.table {tableName}` pattern
    let (|AzureTable|_|) = function
        | Apply("AzureTable.table", SynExpr.Const(SynConst.String(table, tableRange), constRange), range, appRange) ->
            Some (table, constRange)
        | _ ->
            None

    let (|LiteralQuery|_|) = function
        | Apply("AzureTable.table", SynExpr.Ident(identifier), funcRange, appRange) ->
            Some (identifier.idText, funcRange)
        | _ ->
            None

    let rec findTable = function
        | AzureTable (table, range) ->
            [ AzureAnalyzerBlock.Table(table, range) ]
        | SynExpr.App(exprAtomic, isInfix, funcExpr, argExpr, range) ->
            [ yield! findTable funcExpr; yield! findTable argExpr ]
        | _ ->
            [ ]

    let rec findFunc = function
        | SynExpr.App(exprAtomic, isInfix, funcExpr, argExpr, range) ->
            [ yield! findFunc funcExpr; yield! findFunc argExpr ]
        | _ ->
            [ ]

    let rec findFilters = function
        | AzureFilters(filters, range) ->
            let filters =
                filters
                |> List.map (fun (name, range, func, funcRange, appRange) -> { name = name.Trim().TrimStart('@'); range = range; paramFunc = func; paramFuncRange = funcRange; applicationRange = appRange })
            [ AzureAnalyzerBlock.Filters(filters, range) ]

        | SynExpr.App(exprAtomic, isInfix, funcExpr, argExpr, range) ->
            [ yield! findFilters funcExpr; yield! findFilters argExpr ]

        | _ ->
            [ ]

    let rec findExecuteTransaction = function

        | SynExpr.App(exprAtomic, isInfix, funcExpr, argExpr, range) ->
            [
                yield! findExecuteTransaction funcExpr;
                yield! findExecuteTransaction argExpr
            ]

        | _ ->
            [ ]


    let rec findReadColumnAttempts = function
        | ReadColumnAttempt (attempt) ->
            [ attempt ]
        | SynExpr.App(exprAtomic, isInfix, funcExpr, argExpr, range) ->
            [ yield! findReadColumnAttempts funcExpr; yield! findReadColumnAttempts argExpr ]
        | SynExpr.Paren(expr, leftRange, rightRange, range) ->
            [ yield! findReadColumnAttempts expr ]
        | SynExpr.Lambda(fromMethod, inLambdaSeq, args, body,_, range) ->
            [ yield! findReadColumnAttempts body ]
        | SynExpr.LetOrUse(isRecursive, isUse, bindings, body, range) ->
             [ yield! findReadColumnAttempts body
               for binding in bindings do
               match binding with
               | SynBinding.Binding (access, kind, mustInline, isMutable, attrs, xmlDecl, valData, headPat, returnInfo, expr, range, seqPoint) ->
                   yield! findReadColumnAttempts expr ]

        | SynExpr.LetOrUseBang(sequencePoint, isUse, isFromSource, syntaxPattern, expr1, andExprs, body, range) ->
            [
                yield! findReadColumnAttempts expr1
                yield! findReadColumnAttempts body
                for (pointInfo, _, _, pattern, expr, range) in andExprs do
                    yield! findReadColumnAttempts expr
            ]

        | SynExpr.CompExpr(isArray, _, expression, range) ->
            [ yield! findReadColumnAttempts expression ]

        | SynExpr.AnonRecd(isStruct, copyInfo, recordFields, range) ->
            [
                match copyInfo with
                | Some(expr, info) -> yield! findReadColumnAttempts expr
                | None -> ()

                for (fieldName, fieldBody) in recordFields do
                    yield! findReadColumnAttempts fieldBody
            ]

        | SynExpr.ArrayOrList(isList, elements, range) ->
            [
                for elementExpr in elements do
                    yield! findReadColumnAttempts elementExpr
            ]

        | SynExpr.ArrayOrListOfSeqExpr (isArray, body, range) ->
            [
                yield! findReadColumnAttempts body
            ]

        | SynExpr.Record(info, copyInfo, recordFields, range) ->
            [
                for (fieldName, body, blockSep) in recordFields do
                    match body with
                    | Some bodyExpr ->  yield! findReadColumnAttempts bodyExpr
                    | None -> ()
            ]

        | SynExpr.IfThenElse(ifExpr, elseExpr, thenExpr, _, _, _, _) ->
            [
                yield! findReadColumnAttempts ifExpr
                yield! findReadColumnAttempts elseExpr
                match thenExpr with
                | Some expr -> yield! findReadColumnAttempts expr
                | None -> ()
            ]

        | SynExpr.Lambda (fromMethod,inSeq,args,body,parsedData,range) ->
            [
                yield! findReadColumnAttempts body
            ]

        | SynExpr.Lazy(body, range) ->
            [
                yield! findReadColumnAttempts body
            ]

        | SynExpr.New(protocol, typeName, expr, range) ->
            [
                yield! findReadColumnAttempts expr
            ]

        | SynExpr.Tuple (isStruct, exprs, commaRanges, range) ->
            [
                for expr in exprs do
                    yield! findReadColumnAttempts expr
            ]

        | SynExpr.Match(seqPoint, matchExpr, clauses, range) ->
            [
                yield! findReadColumnAttempts matchExpr
                for SynMatchClause.Clause(pattern, whenExpr, body, range, seqPoint) in clauses do
                    yield! findReadColumnAttempts body
                    match whenExpr with
                    | Some body -> yield! findReadColumnAttempts body
                    | None -> ()
            ]

        | SynExpr.MatchBang(seqPoint, matchExpr, clauses, range) ->
            [
                yield! findReadColumnAttempts matchExpr
                for SynMatchClause.Clause(pattern, whenExpr, body, range, seqPoint) in clauses do
                    yield! findReadColumnAttempts body
                    match whenExpr with
                    | Some body -> yield! findReadColumnAttempts body
                    | None -> ()
            ]

        | _ ->
            [ ]


    let rec visitSyntacticExpression (expr: SynExpr) (fullExpressionRange: range) =
        match expr with
        | SynExpr.CompExpr(isArrayOrList, _, innerExpr, range) ->
            visitSyntacticExpression innerExpr range
        | SynExpr.YieldOrReturn(_, innerExpr, innerRange) ->
            visitSyntacticExpression innerExpr innerRange
        | SynExpr.YieldOrReturnFrom(_, innerExpr, innerRange) ->
            visitSyntacticExpression innerExpr innerRange
        | SynExpr.App(exprAtomic, isInfix, funcExpr, argExpr, range) ->
            match argExpr with
            | SynExpr.CompExpr(isArrayOrList, _, innerExpr, range) ->
                visitSyntacticExpression innerExpr range

            | Apply(("AzureTable.execute"|"AzureTable.executeDirect"), lambdaExpr, funcRange, appRange) ->
                let columns = findReadColumnAttempts lambdaExpr
                let blocks = [
                    yield! findTable funcExpr
                    yield! findFilters funcExpr
                    yield! findFunc funcExpr
                    yield AzureAnalyzerBlock.ReadingColumns columns
                ]

                [ { blocks = blocks; range = range; } ]

            | AzureTable(table, tableRange) ->

                let blocks = [
                    AzureAnalyzerBlock.Table(table, tableRange)
                ]

                [ { blocks = blocks; range = range; } ]

            | AzureFilters(filters, range) ->
                let azureFilters =
                    filters
                    |> List.map (fun (name, range, func, funcRange, appRange) -> { name = name.Trim().TrimStart('@'); range = range; paramFunc = func; paramFuncRange = funcRange; applicationRange = appRange })

                let blocks = [
                    yield! findTable funcExpr
                    yield AzureAnalyzerBlock.Filters(azureFilters, range)
                ]

                [ { blocks = blocks; range = range; } ]

            | FuncName(functionWithoutParameters) ->
                let blocks = [
                    yield! findFunc funcExpr
                    yield! findTable funcExpr
                    yield! findFilters funcExpr
                ]

                [ { blocks = blocks; range = range; } ]

            | Apply(anyOtherFunction, functionArg, range, appRange) ->
                let blocks = [
                    yield! findFunc funcExpr
                    yield! findTable funcExpr
                    yield! findFilters funcExpr
                    yield AzureAnalyzerBlock.ReadingColumns (findReadColumnAttempts funcExpr)
                ]

                [ { blocks = blocks; range = range; } ]
            | SynExpr.Paren(innerExpr, leftRange, rightRange, range) ->
                visitSyntacticExpression innerExpr range
            | _ ->
                [ ]
        | SynExpr.LetOrUse(isRecursive, isUse, bindings, body, range) ->
            [
                yield! visitSyntacticExpression body range
                for binding in bindings do yield! visitBinding binding
            ]

        | SynExpr.App(flag, _, SynExpr.Ident ident, SynExpr.CompExpr(_, _, innerExpr, innerExprRange), r) when ident.idText = "async" ->
            visitSyntacticExpression innerExpr innerExprRange

        | SynExpr.LetOrUseBang(_, isUse, isFromSource, pat, rhs, andBangs, body, range) ->
            [
                yield! visitSyntacticExpression body range
                yield! visitSyntacticExpression rhs range
            ]

        | SynExpr.IfThenElse(ifExpr, thenExpr, elseExpr, _, _ ,ifToThenRange, range) ->
            [
                yield! visitSyntacticExpression ifExpr ifToThenRange
                yield! visitSyntacticExpression thenExpr range
                match elseExpr with
                | None -> ()
                | Some expr -> yield! visitSyntacticExpression expr range
            ]

        | SynExpr.Lambda (fromMethod,inSeq,args,body,parsedData,range) ->
            visitSyntacticExpression body range

        | SynExpr.Sequential (debugSeqPoint, isTrueSeq, expr1, expr2, range) ->
            [
                yield! visitSyntacticExpression expr1 range
                yield! visitSyntacticExpression expr2 range
            ]

        | otherwise ->
            [ ]

    and visitBinding (binding: SynBinding) : AzureOperation list =
        match binding with
        | SynBinding.Binding (access, kind, mustInline, isMutable, attrs, xmlDecl, valData, headPat, returnInfo, expr, range, seqPoint) ->
            visitSyntacticExpression expr range

    let findLiterals (ctx: AzureTableAnalyzerContext) =
        let values = new ResizeArray<string * string>()
        for symbol in ctx.Symbols |> Seq.collect (fun s -> s.TryGetMembersFunctionsAndValues) do
            match symbol.LiteralValue with
            | Some value when value.GetType() = typeof<string> ->
                values.Add((symbol.LogicalName, unbox<string> value))
            | _ -> ()

        Map.ofSeq values

    /// Tries to replace [<Literal>] strings inside the module with the identifiers that were used with Sql.query.
    let applyLiterals (literals: Map<string, string>) (operation: AzureOperation) =
        let modifiedBlocks =
            operation.blocks
            |> List.choose (function
                | differentBlock ->
                    Some differentBlock)

        { operation with blocks = modifiedBlocks }
    let findAzureOperations (ctx: AzureTableAnalyzerContext) =
        let operations = ResizeArray<AzureOperation>()
        match ctx.ParseTree with
        | ParsedInput.ImplFile input ->
            match input with
            | ParsedImplFileInput.ParsedImplFileInput(fileName, isScript, qualifiedName, _, _, modules, _) ->
                for parsedModule in modules do
                    match parsedModule with
                    | SynModuleOrNamespace(identifier, isRecursive, kind, declarations, _, _, _, _) ->
                        let rec iterTypeDefs defs =
                            for def in defs do
                                match def with
                                | SynTypeDefn.TypeDefn(typeInfo, typeRepr, members, range) ->
                                    for memberDefn in members do
                                        match memberDefn with
                                        | SynMemberDefn.Member (binding, _) ->
                                            operations.AddRange (visitBinding binding)
                                        | SynMemberDefn.LetBindings (bindings, _, _, _) ->
                                            for binding in bindings do
                                                operations.AddRange (visitBinding binding)
                                        | SynMemberDefn.NestedType (nestedTypeDef, _, _) ->
                                            iterTypeDefs [ nestedTypeDef ]
                                        | _ ->
                                            ()

                                    match typeRepr with
                                    | SynTypeDefnRepr.ObjectModel (modelKind, members, range) ->
                                        for memberDefn in members do
                                            match memberDefn with
                                            | SynMemberDefn.Member (binding, _) ->
                                                operations.AddRange (visitBinding binding)
                                            | SynMemberDefn.LetBindings (bindings, _, _, _) ->
                                                for binding in bindings do
                                                    operations.AddRange (visitBinding binding)
                                            | SynMemberDefn.NestedType (nestedTypeDef, _, _) ->
                                                iterTypeDefs [ nestedTypeDef ]
                                            | _ ->
                                                ()
                                    | _ ->
                                        ()

                        let rec iterDeclarations decls =
                            for declaration in decls do
                                match declaration with
                                | SynModuleDecl.Let(isRecursiveDef, bindings, range) ->
                                    for binding in bindings do
                                        operations.AddRange (visitBinding binding)
                                | SynModuleDecl.NestedModule(moduleInfo, isRecursive, nestedDeclarations, _, _) ->
                                    iterDeclarations nestedDeclarations

                                | SynModuleDecl.Types(definitions, range)  ->
                                    iterTypeDefs definitions

                                | SynModuleDecl.DoExpr(debugInfo, expression, range) ->
                                    operations.AddRange (visitSyntacticExpression expression range)
                                | _ ->
                                    ()

                        iterDeclarations declarations

        | ParsedInput.SigFile file ->
            ()

        let moduleLiterals = findLiterals ctx

        operations
        |> Seq.map (applyLiterals moduleLiterals)
        |> Seq.filter (fun operation -> not (List.isEmpty operation.blocks))
        |> Seq.toList
