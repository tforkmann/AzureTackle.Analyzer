module SyntaxAnalysisReferencingQueryDoesNotGiveError

open Npgsql
open AzureTackle
// this shouldn't be analyzed whe n query is dynamic

let deleteUsers connection =

    let usersQuery = "DELETE FROM users WHERE user_id = @user_id"

    connection
    |> Sql.query usersQuery
    |> Sql.parameters [ "@user_id", Sql.int 42 ]
    |> Sql.executeNonQuery
