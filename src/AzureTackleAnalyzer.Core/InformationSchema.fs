namespace AzureTackle.Analyzers.Core

open System
open System.Data
open System.Data.Common
open System.Collections.Generic

open FSharp.Quotations
open Microsoft.WindowsAzure.Storage.Table
open AzureTackle
open System.Collections
open System.Net
open Microsoft.WindowsAzure.Storage.Table
open FSharp.Control.Tasks.ContextInsensitive

module InformationSchema =


    type internal Type with
        member this.PartiallyQualifiedName =
            sprintf "%s, %s" this.FullName (this.Assembly.GetName().Name)

    // //https://www.postgresql.org/docs/current/static/datatype.html#DATATYPE-TABLE
    // let private builtins = [
    //     "boolean", typeof<bool>; "bool", typeof<bool>

    //     "smallint", typeof<int16>; "int2", typeof<int16>
    //     "integer", typeof<int32>; "int", typeof<int32>; "int4", typeof<int32>
    //     "bigint", typeof<int64>; "int8", typeof<int64>

    //     "real", typeof<single>; "float4", typeof<single>
    //     "double precision", typeof<double>; "float8", typeof<double>

    //     "numeric", typeof<decimal>; "decimal", typeof<decimal>
    //     "money", typeof<decimal>
    //     "text", typeof<string>

    //     "character varying", typeof<string>; "varchar", typeof<string>
    //     "character", typeof<string>; "char", typeof<string>

    //     "citext", typeof<string>
    //     "jsonb", typeof<string>
    //     "json", typeof<string>
    //     "xml", typeof<string>
    //     "point", typeof<NpgsqlPoint>
    //     "lseg", typeof<NpgsqlLSeg>
    //     "path", typeof<NpgsqlPath>
    //     "polygon", typeof<NpgsqlPolygon>
    //     "line", typeof<NpgsqlLine>
    //     "circle", typeof<NpgsqlCircle>
    //     "box", typeof<bool>

    //     "bit", typeof<BitArray>; "bit(n)", typeof<BitArray>; "bit varying", typeof<BitArray>; "varbit", typeof<BitArray>

    //     "hstore", typeof<IDictionary>
    //     "uuid", typeof<Guid>
    //     "cidr", typeof<ValueTuple<IPAddress, int>>
    //     "inet", typeof<IPAddress>
    //     "macaddr", typeof<NetworkInformation.PhysicalAddress>
    //     "tsquery", typeof<NpgsqlTsQuery>
    //     "tsvector", typeof<NpgsqlTsVector>

    //     "date", typeof<DateTime>
    //     "interval", typeof<TimeSpan>
    //     "timestamp without time zone", typeof<DateTime>; "timestamp", typeof<DateTime>
    //     "timestamp with time zone", typeof<DateTime>; "timestamptz", typeof<DateTime>
    //     "time without time zone", typeof<TimeSpan>; "time", typeof<TimeSpan>
    //     "time with time zone", typeof<DateTimeOffset>; "timetz", typeof<DateTimeOffset>

    //     "bytea", typeof<byte[]>
    //     "oid", typeof<UInt32>
    //     "xid", typeof<UInt32>
    //     "cid", typeof<UInt32>
    //     "oidvector", typeof<UInt32[]>
    //     "name", typeof<string>
    //     "char", typeof<string>
    //     //"range", typeof<NpgsqlRange>, NpgsqlDbType.Range)
    // ]

    // let getTypeMapping =
    //     let allMappings = dict builtins
    //     fun datatype ->
    //         let exists, value = allMappings.TryGetValue(datatype)
    //         if exists then value else failwithf "Unsupported datatype %s." datatype



    // type DataType = {
    //     Name: string
    //     Schema: string
    //     IsArray : bool
    //     ClrType: Type
    // }   with
    //     member this.FullName = sprintf "%s.%s" this.Schema this.Name
    //     member this.IsUserDefinedType = this.Schema <> "pg_catalog"
    //     member this.IsFixedLength = this.ClrType.IsValueType
    //     member this.UdtTypeName =
    //         if this.ClrType.IsArray
    //         then
    //             let withoutTrailingBrackets = this.Name.Substring(0, this.Name.Length - 2) // my_enum[] -> my_enum
    //             sprintf "%s.%s" this.Schema withoutTrailingBrackets
    //         else this.FullName

    //     static member Create(x: PostgresTypes.PostgresType) =
    //         let clrType = x.ToClrType()
    //         {
    //             Name = x.Name
    //             Schema = x.Namespace
    //             ClrType = clrType
    //             IsArray = clrType.IsArray
    //         }

    // type Schema =
    //     { OID : string
    //       Name : string }

    type TableInfo =
        { ColumnName : string
          EntityProperty : EntityProperty }

    type Column =
        { ColumnAttributeNumber : int16
          Name: string
          EntityProperty: EntityProperty
          Nullable: bool
          MaxLength: int
          ReadOnly: bool
          AutoIncrement: bool
          DefaultConstraint: string
          Description: string
          UDT: Type option Lazy // must be lazt due to late binding of columns with user defined types.
          PartOfPrimaryKey: bool
          BaseSchemaName: string
          BaseTableName: string }
        // with

        // member this.ClrType = this.EntityProperty.

        // member this.ClrTypeConsideringNullability =
        //     if this.Nullable
        //     then typedefof<_ option>.MakeGenericType this.DataType.ClrType
        //     else this.DataType.ClrType

        // member this.HasDefaultConstraint = string this.DefaultConstraint <> ""
        // member this.OptionalForInsert = this.Nullable || this.HasDefaultConstraint || this.AutoIncrement

    // type DbEnum =
    //     { Name: string
    //       Values: string list }

    // type DbSchemaLookupItem =
    //     { Schema : Schema
    //       Tables : Dictionary<Table, HashSet<Column>>
    //       Enums : Map<string, DbEnum> }

    // type ColumnLookupKey = { TableOID : string; ColumnAttributeNumber : int16 }

    type TableEntity =
        { Entity : DynamicTableEntity option}

    type Filters =
        { Name: string
          Direction: ParameterDirection
          MaxLength: int
          Precision: byte
          Scale : byte
          Optional: bool
          EntityProperty: EntityProperty
          IsNullable : bool }
        with

        member this.Size = this.MaxLength

    // let inline openConnection connectionString =
    //     let conn = new NpgsqlConnection(connectionString)
    //     conn.Open()
    //     conn
    let getAzureTableEntity(connectionString,table) =
        task {
        let azureProps =
            AzureTable.connect connectionString
            |> AzureTable.table table

        let azureTable =
            match azureProps.AzureTable with
            | Some table -> table
            | None -> failwith "please add a table"
        let filter = AzureTable.appendFilters azureProps.Filters
        let! results =
            Table.getResultsRecursivly filter azureTable
        let entity = results |> List.tryHead
        return { Entity = entity }
    }
    let columnDict = Dictionary<string, EntityProperty>()

    let extractTableInfo (connectionString,table) =
        task {
            let! entity = getAzureTableEntity(connectionString,table)

            match entity.Entity with
            | Some ent ->
                ent.Properties
                |> Seq.iter (fun keyPair -> columnDict.Add(keyPair.Key, keyPair.Value))
            | None -> failwithf "Could not get an entity"
            return
                columnDict
                |> Seq.map (fun column ->
                    { ColumnName = column.Key
                      EntityProperty = column.Value})
                |> Seq.toArray

        }
    // let extractParametersAndOutputColumns(connectionString, commandText, allParametersOptional, dbSchemaLookups : DbSchemaLookups) =
    //     use conn = openConnection(connectionString)

    //     use cmd = new NpgsqlCommand(commandText, conn)
    //     NpgsqlCommandBuilder.DeriveParameters(cmd)
    //     for p in cmd.Parameters do p.Value <- DBNull.Value
    //     let cols =
    //         use cursor = cmd.ExecuteReader(CommandBehavior.SchemaOnly)
    //         if cursor.FieldCount = 0 then [] else [ for c in cursor.GetColumnSchema() -> c ]

    //     let outputColumns =
    //         [ for column in cols ->
    //             let columnAttributeNumber = column.ColumnAttributeNumber.GetValueOrDefault(-1s)

    //             if column.TableOID <> 0u then
    //                 let lookupKey = { TableOID = string column.TableOID
    //                                   ColumnAttributeNumber = columnAttributeNumber }
    //                 { dbSchemaLookups.Columns.[lookupKey] with Name = column.ColumnName }
    //             else
    //                 let dataType = DataType.Create(column.PostgresType)

    //                 {
    //                     ColumnAttributeNumber = columnAttributeNumber
    //                     Name = column.ColumnName
    //                     DataType = dataType
    //                     Nullable = column.AllowDBNull.GetValueOrDefault(true)
    //                     MaxLength = column.ColumnSize.GetValueOrDefault(-1)
    //                     ReadOnly = true
    //                     AutoIncrement = column.IsIdentity.GetValueOrDefault(false)
    //                     DefaultConstraint = column.DefaultValue
    //                     Description = ""
    //                     UDT = lazy None
    //                     PartOfPrimaryKey = column.IsKey.GetValueOrDefault(false)
    //                     BaseSchemaName = column.BaseSchemaName
    //                     BaseTableName = column.BaseTableName
    //                 }  ]


    //     let parameters =
    //         [ for p in cmd.Parameters ->
    //             assert (p.Direction = ParameterDirection.Input)
    //             { Name = p.ParameterName
    //               NpgsqlDbType =
    //                 match p.PostgresType with
    //                 | :? PostgresArrayType as x when (x.Element :? PostgresEnumType) ->
    //                     //probably array of custom type (enum or composite)
    //                     NpgsqlDbType.Array ||| NpgsqlDbType.Text
    //                 | _ -> p.NpgsqlDbType
    //               Direction = p.Direction
    //               MaxLength = p.Size
    //               Precision = p.Precision
    //               Scale = p.Scale
    //               Optional = allParametersOptional
    //               DataType = DataType.Create(p.PostgresType)
    //               IsNullable = true } ]

    //     let enums =
    //         outputColumns
    //         |> Seq.choose (fun c ->
    //             if c.DataType.IsUserDefinedType && dbSchemaLookups.Enums.ContainsKey(c.DataType.UdtTypeName) then
    //                 Some (c.DataType.UdtTypeName, dbSchemaLookups.Enums.[c.DataType.UdtTypeName])
    //             else
    //                 None)
    //         |> Seq.append [
    //             for p in parameters do
    //                 if p.DataType.IsUserDefinedType && dbSchemaLookups.Enums.ContainsKey(p.DataType.UdtTypeName)
    //                 then
    //                     yield p.DataType.UdtTypeName, dbSchemaLookups.Enums.[p.DataType.UdtTypeName]
    //         ]
    //         |> Seq.distinct
    //         |> Map.ofSeq

    //     parameters, outputColumns, enums


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
