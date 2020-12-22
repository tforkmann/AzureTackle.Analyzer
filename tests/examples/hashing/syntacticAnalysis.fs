module AzureTable

open AzureTackle
open Config

let findDatesForOne (rowKey: string) =
    connectionString
    |> AzureTable.connect
    |> AzureTable.table "TestTable"
    |> AzureTable.filter [ RoKey(Equal, rowKey) ]
    |> AzureTable.executeDirect (fun read -> read.dateTimeOffset "Date")

let findRowKeyInTestData =
    connectionString
    |> AzureTable.connect
    |> AzureTable.table "TestTable"
    |> AzureTable.executeDirect (fun read -> read.rowKey)
