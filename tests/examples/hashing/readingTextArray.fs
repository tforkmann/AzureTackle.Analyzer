module ReadingTextArray

open AzureTackle

let findRoles connection =
    connection
    |> Sql.query "SELECT roles FROM users"
    |> Sql.execute (fun read -> read.string "roles") // should be read.stringArray
