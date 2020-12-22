module Config
open System.IO
open Microsoft.Extensions.Configuration

let config =
    ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("AZURE_TACKLE.json")
        .Build()

let connectionString = config.["StorageConnectionString"]
