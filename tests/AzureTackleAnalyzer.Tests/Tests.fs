module Tests

open System
open Expecto
open AzureTackle.Analyzers
open AzureTackle.Analyzers.Core
open AzureTackle
open FSharp.Analyzers.SDK

let analyzers = [ AzureTableAnalyzer.tableAnalyzer ]

let inline find file =
    IO.Path.Combine(__SOURCE_DIRECTORY__, file)

let project =
    IO.Path.Combine(__SOURCE_DIRECTORY__, "../examples/hashing/examples.fsproj")

let raiseWhenFailed =
    function
    | Ok _ -> ()
    | Result.Error error -> raise error


let connectionString =
    "DefaultEndpointsProtocol=https;AccountName=dptestchiadev;AccountKey=Vd64M6dPKvW/yRQ32xvptAJWV0GGlaeZJxkArJ8ZGJEKWx/aZH5KAxMMPHJkeL/gMiJb65krq8S5yRxCK67p8w==;BlobEndpoint=https://dptestchiadev.blob.core.windows.net/;QueueEndpoint=https://dptestchiadev.queue.core.windows.net/;TableEndpoint=https://dptestchiadev.table.core.windows.net/;FileEndpoint=https://dptestchiadev.file.core.windows.net/;"

[<Literal>]
let TestTable = "TestTable"

type TestData =
    { PartKey: string
      RowKey: RowKey
      Date: DateTimeOffset
      Exists: bool
      Value: float
      Text: string }

[<Tests>]
let tests toolsPath =
    let opts = AnalyzerBootstrap.getOpts (find project) toolsPath
    let inline context file =
        AnalyzerBootstrap.context opts file  []
        |> Option.map AzureTableAnalyzer.azureTableAnalyzerContext

    testList
        "AzureTable"
        [

          testTask "Syntactic Analysis: AzureTable blocks can be detected with their relavant information" {
              match context (find "../examples/hashing/syntacticAnalysis.fs") with
              | None ->
                  failwith "Could not crack project"
              | Some context ->
                  let operationBlocks =
                      SyntacticAnalysis.findAzureOperations context

                  Expect.equal 2 (List.length operationBlocks) "Found 11 operation blocks"
          }
          testTask "Syntactic Analysis: Azure filters can be analyzed" {
              match context (find "../examples/hashing/readingAzureTable.fs") with
              | None ->
                  failwith "Could not crack project"
              | Some context ->
                  match SyntacticAnalysis.findAzureOperations context with
                  | [operation] ->
                    // printfn "Operation %A" operation
                    let filters =
                        operation.blocks
                        |> List.tryPick (fun block ->
                            match block with
                            | AzureAnalyzerBlock.Filters (filters,_) -> Some filters
                            | _ -> None)
                    match filters with
                    | None -> failwith "Expected filters to be found"
                    | Some [ ] -> failwith "Expected filters to have one query"
                    | Some (f) ->
                        Expect.equal 1 f.Length "There is one parameter set"
                  | _ ->
                    failwith "Should not happen"
          } ]
