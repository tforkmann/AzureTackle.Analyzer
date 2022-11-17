namespace AzureTackle.Analyzers.Core

open System.Collections.Generic
open Microsoft.Azure.Cosmos.Table
open AzureTackle
open FSharp.Control.Tasks.ContextInsensitive

module InformationSchema =

    type TableInfo =
        { ColumnName: string
          Nullable : bool
          EntityProperty: EntityProperty }

    type TableList = { Tables: CloudTable list }
    type TableEntity = { Entity: DynamicTableEntity option }

    let getTablesRecursivly (storageAccount: CloudStorageAccount) =
        task {
            try
                let rec getTables token =
                    task {
                        let tableClient = storageAccount.CreateCloudTableClient()
                        let! result = tableClient.ListTablesSegmentedAsync(token)
                        let token = result.ContinuationToken
                        let result = result |> Seq.toList

                        if isNull token then
                            return result
                        else
                            let! others = getTables token
                            return result @ others
                    }

                return! getTables null
            with
            | exn -> return failwithf "Could not get tables from storage account %A %s" storageAccount exn.Message

        }

    let getAzureTables (connectionString:string) =
        task {
            try
                let storageAccount = CloudStorageAccount.Parse connectionString
                let! tables = getTablesRecursivly storageAccount
                return { Tables = tables }
            with
            | exn-> return failwithf "Could not get tables %s" exn.Message
        }

    let getAzureTableEntity (connectionString, table) =
        task {
            let azureProps =
                AzureTable.connect connectionString
                |> AzureTable.table table

            let azureTable = AzureTable.getTable azureProps

            let filter =
                AzureTable.appendFilters azureProps.Filters

            let! results = Table.getResultsRecursivly filter azureTable
            let entity = results |> List.tryHead
            return { Entity = entity }
        }

    let columnDict = Dictionary<string, EntityProperty>()

    let extractTableInfo (connectionString, tableName ,availableTables) =
        task {
            try

                let! entity = getAzureTableEntity (connectionString, tableName)

                match entity.Entity with
                | Some ent ->
                    ent.Properties
                    |> Seq.iter (fun keyPair -> columnDict.Add(keyPair.Key, keyPair.Value))
                | None -> return failwithf "Could not get an entity from table %s. Please use on of the following tables [%s]" tableName availableTables
                let partAndRowKey  =
                    [{ColumnName = "RoKey"
                      Nullable = false
                      EntityProperty =  EntityProperty.GeneratePropertyForString entity.Entity.Value.RowKey}
                     {ColumnName = "PaKey"
                      Nullable = false
                      EntityProperty = EntityProperty.GeneratePropertyForString entity.Entity.Value.PartitionKey}]
                let tableInfos =
                    columnDict
                    |> Seq.map
                        (fun column ->

                            { ColumnName = column.Key
                              Nullable = false //TODO: Find out if nullable
                              EntityProperty = column.Value })
                    |> Seq.toList
                return List.concat [partAndRowKey; tableInfos]
            with
            | _ -> return failwithf "Could not get entity for table %s. Available tables are [%s]" tableName availableTables

        }
