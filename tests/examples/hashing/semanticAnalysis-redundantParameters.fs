module semanticAnalysis_redundantParameters

open AzureTackle
open AzureTackle.OptionWorkflow

let connectionString = "Dummy connection string"

let findUsers() =
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT * FROM users"
    |> Sql.parameters [ "user_id", Sql.int 42 ]
    |> Sql.execute (fun read -> read.text "username")
