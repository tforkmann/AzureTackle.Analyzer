module ReadingAzureTableNoFilter

open AzureTackle
open Config
let findTestData =
    connectionString
    |> AzureTable.connect
    |> AzureTable.table "TestTable"
    |> AzureTable.executeDirect (fun read -> read.string "roles")
