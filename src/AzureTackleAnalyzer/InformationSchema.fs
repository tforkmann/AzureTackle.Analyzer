namespace AzureTackle.Analyzers.Core

open System.Collections.Generic
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
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
        }

    let getAzureTables (connectionString:string) =
        task {
            let storageAccount = CloudStorageAccount.Parse connectionString
            let! tables = getTablesRecursivly storageAccount
            return { Tables = tables }
        }

    let getAzureTableEntity (connectionString, table) =
        task {
            let azureProps =
                AzureTable.connect connectionString
                |> AzureTable.table table

            let azureTable =
                match azureProps.AzureTable with
                | Some table -> table
                | None -> failwith "please add a table"

            let filter =
                AzureTable.appendFilters azureProps.Filters

            let! results = Table.getResultsRecursivly filter azureTable
            let entity = results |> List.tryHead
            return { Entity = entity }
        }

    let columnDict = Dictionary<string, EntityProperty>()

    let extractTableInfo (connectionString, table) =
        task {
            let! entity = getAzureTableEntity (connectionString, table)

            match entity.Entity with
            | Some ent ->
                ent.Properties
                |> Seq.iter (fun keyPair -> columnDict.Add(keyPair.Key, keyPair.Value))
            | None -> failwithf "Could not get an entity"

            return
                columnDict
                |> Seq.map
                    (fun column ->

                        { ColumnName = column.Key
                          Nullable = false //TODO: Find out if nullable
                          EntityProperty = column.Value })
                |> Seq.toList

        }
