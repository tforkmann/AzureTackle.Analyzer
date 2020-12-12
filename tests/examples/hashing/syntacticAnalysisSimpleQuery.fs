module syntacticAnalysisSimpleQuery

open AzureTackle
open AzureTackle.OptionWorkflow

let connectionString = "Dummy connection string"

let findUsernames() =
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT COUNT(*) FROM users"
