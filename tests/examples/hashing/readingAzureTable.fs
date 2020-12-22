module ReadingAzureTable

open AzureTackle
open Config
let findTestData =
    connectionString
    |> AzureTable.connect
    |> AzureTable.table "TestTable"
    |> AzureTable.filter [ RoKey(Equal, "") ]
    |> AzureTable.executeDirect (fun read -> read.string "roles")
