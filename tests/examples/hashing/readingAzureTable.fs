module ReadingAzureTable

open AzureTackle

let findTestData connection table =
    AzureTable.connect connection
    |> AzureTable.table table
    |> AzureTable.executeDirect (fun read -> read.string "roles")
