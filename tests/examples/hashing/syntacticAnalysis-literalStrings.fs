module semanticAnalysis_literalStrings

open AzureTackle

let connectionString = "Dummy connection string"

let [<Literal>] query = "SELECT * FROM users"

let [<Literal>] maskedQuery = query

let findUsers() =
    connectionString
    |> Sql.connect
    |> Sql.query maskedQuery
    |> Sql.executeAsync (fun read -> read.text "username")
