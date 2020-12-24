module ReadingAzureTable

open AzureTackle
open Config
let findTestData =
    connectionString
    |> AzureTable.connect
    |> AzureTable.table "TestTabl"
    |> AzureTable.filter [ RoKey(Equal, "RowKey") ]
    |> AzureTable.executeDirect (fun read -> read.string "roles")
