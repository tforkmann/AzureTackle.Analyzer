namespace AzureTackle.Analyzers

open FSharp.Analyzers.SDK
open System
open FSharp.Control.Tasks.ContextInsensitive

module AzureTableAnalyzer =

    let specializedSeverity =
        function
        | Core.Severity.Error -> Error
        | Core.Severity.Info -> Info
        | Core.Severity.Warning -> Warning

    let specializedFix (fix: Core.Fix): Fix =
        { FromRange = fix.FromRange
          FromText = fix.FromText
          ToText = fix.ToText }

    let specializedMessage (message: Core.Message): Message =
        { Code = message.Code
          Fixes = message.Fixes |> List.map specializedFix
          Message = message.Message
          Range = message.Range
          Severity = message.Severity |> specializedSeverity
          Type = message.Type }

    let azureTableAnalyzerContext (ctx: Context): Core.AzureTableAnalyzerContext =
        { Content = ctx.Content
          FileName = ctx.FileName
          Symbols = ctx.Symbols
          ParseTree = ctx.ParseTree }

    [<Analyzer "AzureTackle.Analyzer">]
    let tableAnalyzer: Analyzer =
        fun (ctx: Context) ->
            task {
                let syntacticBlocks =
                    Core.SyntacticAnalysis.findAzureOperations (azureTableAnalyzerContext ctx)

                if List.isEmpty syntacticBlocks then
                    return []
                else
                    let connectionString = Core.AzureTableAnalyzer.connectionString

                    if isNull connectionString
                       || String.IsNullOrWhiteSpace connectionString then
                        return
                            [ for block in syntacticBlocks ->
                                Core.AzureAnalysis.createWarning
                                    "Missing environment variable 'AZURE_TACKLE'. Please set that variable to the connection string of your azure table put the connection string in a file called 'AZURE_TACKLE' relative next your project or in your project root."
                                    block.range
                                |> specializedMessage ]
                    else
                        let! schema = Core.AzureAnalysis.databaseSchema connectionString
                        printfn "got schema"
                        match schema with
                        | Result.Error connectionError ->
                            return
                                [ for block in syntacticBlocks ->
                                    Core.AzureAnalysis.createWarning
                                        (sprintf
                                            "Error while connecting to the azure table using the connection string %s from environment variable 'AZURE_TACKLE' or put the connection string in a file called 'AZURE_TACKLE' relative next your project or in your project root. Connection error: %s SyntactivBlocks: %A"
                                            connectionString
                                            connectionError
                                            syntacticBlocks)
                                        block.range
                                    |> specializedMessage ]

                        | Result.Ok tableList ->
                            return
                                syntacticBlocks
                                |> List.collect
                                    (fun block -> Core.AzureAnalysis.analyzeOperation block connectionString)
                                |> List.map specializedMessage
                                |> List.distinctBy (fun message -> message.Range)

            }
            |> Async.AwaitTask
            |> Async.RunSynchronously
