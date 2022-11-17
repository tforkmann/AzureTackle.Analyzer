module Tests

open System
open System.IO
open Expecto
open AzureTackle.Analyzers
open AzureTackle.Analyzers.Core
open AzureTackle
open FSharp.Control.Tasks.ContextInsensitive
open Microsoft.Azure.Cosmos.Table
open AzureTackle.Analyzers.Core.InformationSchema
open Microsoft.Extensions.Configuration
let analyzers = [ AzureTableAnalyzer.tableAnalyzer ]

let inline find file =
    IO.Path.Combine(__SOURCE_DIRECTORY__, file)

let project =
    IO.Path.Combine(__SOURCE_DIRECTORY__, "../examples/hashing/examples.fsproj")

let raiseWhenFailed =
    function
    | Ok _ -> ()
    | Result.Error error -> raise error

let config =
    ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("AZURE_TACKLE.json")
        .Build()

let connectionString = config.["StorageConnectionString"]
printfn" connectionString: %s" connectionString
[<Literal>]
let TestTable = "accounting"

type TestData =
    { PartKey: string
      RowKey: RowKey
      Date: DateTimeOffset
      Exists: bool
      Value: float
      Text: string }

[<Tests>]
let tests toolsPath =
    let opts =
        AnalyzerBootstrap.getOpts (find project) toolsPath

    let inline context file =
        AnalyzerBootstrap.context opts file []
        |> Option.map AzureTableAnalyzer.azureTableAnalyzerContext

    testList
        "AzureTable"
        [

          testTask "Syntactic Analysis: AzureTable blocks can be detected with their relavant information" {
              match context (find "../examples/hashing/syntacticAnalysis.fs") with
              | None -> failwith "Could not crack project"
              | Some context ->
                  let operationBlocks =
                      SyntacticAnalysis.findAzureOperations context

                  Expect.equal 2 (List.length operationBlocks) "Found 11 operation blocks"
          }
          testTask "Syntactic Analysis: Azure tables can be analyzed" {
              match context (find "../examples/hashing/readingFaultyAzureTableName.fs") with
              | None -> failwith "Could not crack project"
              | Some context ->
                  let! cloudTable =
                    task{
                        match! AzureAnalysis.databaseSchema connectionString with
                        | Result.Ok schema ->
                            return schema.Tables.[0]
                        | Result.Error connectionError ->
                            return failwith connectionError
                        }
                  let operations = SyntacticAnalysis.findAzureOperations context
                  let tableOperation = operations.Head
                  let messages = AzureAnalysis.analyzeOperation tableOperation connectionString
                  let table : CloudTable = cloudTable
                  Expect.isTrue (messages.[0].Message.Contains table.Name) "Correct table names should be found"

          }
          testTask "Syntactic Analysis: Azure filters can be analyzed" {
              match context (find "../examples/hashing/readingAzureTable.fs") with
              | None ->
                  failwith "Could not crack project"
              | Some context ->
                  match SyntacticAnalysis.findAzureOperations context with
                  | [operation] ->
                    let filters =
                        operation.blocks
                        |> List.tryPick (fun block ->
                            match block with
                            | AzureAnalyzerBlock.Filters (filters,_) -> Some filters
                            | _ -> None)
                    match filters with
                    | None -> failwith "Expected filters to be found"
                    | Some [ ] -> failwith "Expected filters to have at least one filter"
                    | Some (f) ->
                        Expect.equal 2 f.Length "There is one filter set"
                  | _ ->
                    failwith "Should not happen"
          }
          testTask "Syntactic Analysis: simple filter can be read" {
            match context (find "../examples/hashing/readingAzureTable.fs") with
            | None -> failwith "Could not crack project"
            | Some context ->
                match SyntacticAnalysis.findAzureOperations context with
                | [ operation ] ->
                    match AzureAnalysis.findFilters operation with
                    | Some(filter, range) ->
                        Expect.equal filter.Length 2 "Literal string should be read correctly"
                    | None -> failwith "Should have found the correct query"
                | _ ->
                    failwith "Should not happen"
            }
          testTask "Semantic Analysis: empty filter should not give error" {

                match context (find "../examples/hashing/readingAzureTableNoFilter.fs") with
                | None -> failwith "Could not crack project"
                | Some context ->
                    match! AzureAnalysis.databaseSchema connectionString with
                    | Result.Error connectionError ->
                        failwith connectionError
                    | Result.Ok schema ->
                        let block = List.exactlyOne (SyntacticAnalysis.findAzureOperations context)
                        let messages = AzureAnalysis.analyzeOperation block connectionString
                        let filterMessage = messages |> List.find (fun x -> x.Type.Contains "Filter")
                        let messageType = filterMessage.IsInfo()
                        Expect.isTrue messageType "The message is an info"
            }
          ]
