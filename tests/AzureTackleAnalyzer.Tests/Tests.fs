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

let inline context file =
    AnalyzerBootstrap.context file []
    |> Option.map AzureTableAnalyzer.azureTableAnalyzerContext

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
    testList
        "AzureTable"
        [

          testTask "Syntactic Analysis: AzureTable blocks can be detected with their relavant information" {
            //   match context (find "../examples/hashing/syntacticAnalysis.fs") with
              match context (find "../examples/hashing/test.fsx") with
              | None ->
                  printfn "Could not crack project"
                  failwith "Could not crack project"
              | Some context ->
                  printfn "Context"

                  let operationBlocks =
                      SyntacticAnalysis.findAzureOperations context

                  Expect.equal 11 (List.length operationBlocks) "Found 11 operation blocks"
          } ]
