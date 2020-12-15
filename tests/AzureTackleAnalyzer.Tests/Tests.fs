module Tests

open System
open Expecto
open AzureTackle.Analyzers
open AzureTackle.Analyzers.Core
open AzureTackle

let analyzers = [
    AzureTableAnalyzer.tableAnalyzer
]

let inline find file = IO.Path.Combine(__SOURCE_DIRECTORY__ , file)
let project = IO.Path.Combine(__SOURCE_DIRECTORY__, "../examples/hashing/Examples.fsproj")

let raiseWhenFailed = function
    | Ok _ -> ()
    | Result.Error error -> raise error

let inline context file =
    AnalyzerBootstrap.context file
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
let tests =
    testList "AzureTable" [


        testTask "Azure query" {

            let! values =
                  AzureTable.connect connectionString
                  |> AzureTable.table TestTable
                  |> AzureTable.executeDirect (fun read ->
                      { PartKey = read.partKey
                        RowKey = read.rowKey
                        Date = read.dateTimeOffset "Date"
                        Exists = read.bool "Exists"
                        Value = read.float "Value"
                        Text = read.string "Text" })


            let! databaseMetadata = InformationSchema.getAzureTableEntity (connectionString,TestTable)

            let! tableInfo = InformationSchema.extractTableInfo(connectionString,TestTable)
            Expect.equal 4 (tableInfo |> Array.length) "There are 4 columns in users table"
        }


    ]
