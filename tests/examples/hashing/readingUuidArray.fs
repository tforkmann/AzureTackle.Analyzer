module ReadingUuidArray

open AzureTackle

let findRoles connection =
    connection
    |> Sql.query "SELECT codes FROM users"
    |> Sql.execute (fun read -> read.uuid "codes") // should be read.uuidArray
