module ReadingAzureTable

open AzureTackle
open Config
let findTestData =
    connectionString
    |> AzureTable.connect
    |> AzureTable.table "TestTable"
    |> AzureTable.filter [ RoKey(Equal, "RowKey") ]
    |> AzureTable.executeDirect (fun read -> read.string "roles")

let newQuery =
    connectionString
    |> AzureTable.connect
    |> AzureTable.table "TestTable"
    |> AzureTable.executeDirect (fun read -> read.rowKey)
