namespace AzureTackle.Analyzers.Core

open System
open System.Data
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


// let enumsLookup =
//     [
//         use cursor = cmd.ExecuteReader()
//         while cursor.Read() do
//             let schema = cursor.GetString(0)
//             let name = cursor.GetString(1)
//             let values: string[] = cursor.GetValue(2) :?> _
//             let t = { Name = name; Values = List.ofArray values }
//             yield schema, name, t
//     ]
//     |> Seq.groupBy (fun (schema, _, _) -> schema)
//     |> Seq.map (fun (schema, types) ->
//         schema, types |> Seq.map (fun (_, name, t) -> name, t) |> Map.ofSeq
//     )
//     |> Map.ofSeq


// let schemas = Dictionary<string, DbSchemaLookupItem>()
// let columns = Dictionary<ColumnLookupKey, Column>()

// use row = cmd.ExecuteReader()
// while row.Read() do
//     let schema : Schema =
//         { OID = string row.["schema_oid"]
//           Name = string row.["schema_name"] }

//     if not <| schemas.ContainsKey(schema.Name) then

//         schemas.Add(schema.Name, { Schema = schema
//                                    Tables = Dictionary();
//                                    Enums = enumsLookup.TryFind schema.Name |> Option.defaultValue Map.empty })

//     match row.["table_oid"] |> Option.ofObj with
//     | None -> ()
//     | Some oid ->
//         let table =
//             { OID = string oid
//               Name = string row.["table_name"]
//               Description = row.["table_description"] |> Option.ofObj |> Option.map string }

//         if not <| schemas.[schema.Name].Tables.ContainsKey(table) then
//             schemas.[schema.Name].Tables.Add(table, HashSet())

//         match row.GetValueOrDefault("col_number", -1s) with
//         | -1s -> ()
//         | attnum ->
//             let udtName = string row.["col_udt_name"]
//             let isUdt =
//                 schemas.[schema.Name].Enums
//                 |> Map.tryFind udtName
//                 |> Option.isSome

//             let clrType =
//                 match string row.["col_data_type"] with
//                 | "ARRAY" ->
//                     let elemType = getTypeMapping(udtName.TrimStart('_') )
//                     elemType.MakeArrayType()
//                 | "USER-DEFINED" ->
//                     if isUdt then typeof<string> else typeof<obj>
//                 | dataType ->
//                     getTypeMapping(dataType)

//             let column =
//                 let isArray = string row.["col_data_type"] = "ARRAY"
//                 { ColumnAttributeNumber = attnum
//                   Name = string row.["col_name"]
//                   DataType = { Name = if isArray then udtName.TrimStart('_') else udtName
//                                Schema = schema.Name
//                                IsArray = isArray
//                                ClrType = clrType }
//                   Nullable = row.["col_not_null"] |> unbox |> not
//                   MaxLength = row.GetValueOrDefault("col_max_length", -1)
//                   ReadOnly = row.["col_is_updatable"] |> unbox |> not
//                   AutoIncrement = unbox row.["col_is_identity"]
//                   DefaultConstraint = row.GetValueOrDefault("col_default", "")
//                   Description = row.GetValueOrDefault("col_description", "")
//                   UDT = lazy None
//                   PartOfPrimaryKey = unbox row.["col_part_of_primary_key"]
//                   BaseSchemaName = schema.Name
//                   BaseTableName = string row.["table_name"] }

//             if not <| schemas.[schema.Name].Tables.[table].Contains(column) then
//                 schemas.[schema.Name].Tables.[table].Add(column) |> ignore

//             let lookupKey = { TableOID = table.OID
//                               ColumnAttributeNumber = column.ColumnAttributeNumber }
//             if not <| columns.ContainsKey(lookupKey) then
//                 columns.Add(lookupKey, column)
