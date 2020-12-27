# AzureTackle.Analyzer
![.NET](https://github.com/tforkmann/AzureTackle.Analyzer/workflows/.NET/badge.svg)

Analyzer that provides embedded **Azure syntax analysis** when writing queries using [AzureTackle](https://github.com/tforkmann/AzureTackle). It verifies query syntax, checks the parameters in the query match with the provided parameters and performs **type-checking** on the functions that read columns from the result sets.

## Features
- Static query syntax analysis and type-checking against development database
- Detecting missing or redundant parameters
- Detecting parameters with type-mismatch
- Verifying the columns being read from the result set and their types
- Built-in code fixes and nice error messages
- Ability to write multi-line queries in `[<Literal>]` text and referencing it
- Ability to suppress the warnings when you know better than the analyzer ;) 
- Free (MIT licensed)
- Supports VS Code with [Ionide](https://github.com/ionide/ionide-vscode-fsharp) via F# Analyzers SDK

## NuGet

| Package              | Stable                                                                                                                     | Prerelease                                                                                                                                         |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| AzureTackleAnalyzer | [![NuGet Badge](https://buildstats.info/nuget/AzureTackleAnalyzer)](https://www.nuget.org/packages/AzureTackleAnalyzer/) | [![NuGet Badge](https://buildstats.info/nuget/AzureTackleAnalyzer?includePreReleases=true)](https://www.nuget.org/packages/AzureTackleAnalyzer/) |

## Using The Analyzer (VS Code)

### 1 - Configure the connection string to your development database
The analyzer requires a connection string that points to the database you are developing against. You can configure this connection string by either creating a file called `AZURE_TACKLE.json` somewhere next to your F# project or preferably in the root of your workspace. This file should contain that connection string and nothing else. An example of the contents of such file:
```json
{
    "StorageConnectionString": "DefaultEndpointsProtocol=https...."
}
```
> Remember to add an entry in your .gitingore file to make sure you don't commit the connection string to your source version control system.

The analyzer will try to locate and read the file first.

### 2 - Install the analyzer using paket
Use paket to install the analyzer into a specialized `Analyzers` dependency group like this:
```
dotnet paket add AzureTackleAnalyzer --group Analyzers
```
**DO NOT** use `storage:none` because we want the analyzer package to be downloaded physically into `packages/analyzers` directory.

### 3 - Enable analyzers in Ionide
Make sure you have these settings in Ionide for FSharp
```json
{
    "FSharp.enableAnalyzers": true,
    "FSharp.analyzersPath": [
        "./packages/analyzers"
    ]
}
```
Which instructs Ionide to load the analyzers from the directory of the analyzers into which `AzureTackleAnalyzer` was installed.

### See the Analyzer doing it's job

```fs
open AzureTackle

let addBadQuery() =
    connectionString
    |> AzureTable.connect
    |> AzureTable.table
    |> AzureTable.executeDirect (fun read -> read.string "column")
```
Make sure you have added you storage account key in you `AZURE_TACKLE.json` file. Example:

```json
{
    "StorageConnectionString": "DefaultEndpointsProtocol=https...."
}
```

You will see the column is not to correct table entitiy.

### Developing

Make sure the following **requirements** are installed on your system:

- [dotnet SDK](https://www.microsoft.com/net/download/core) 5.0 or higher
- AzureTackle and a Azure storageAccount

### Building


```sh
> build.cmd <optional buildtarget> // on windows
$ ./build.sh  <optional buildtarget>// on unix
```

### Running The Tests

```fs
open AzureTackle

let createTestDatabase() =
    connectionString
    |> AzureTable.connect
    |> AzureTable.table
    |> AzureTable.executeDirect (fun read -> read.string "column")
```
Make sure you have added you storage account key in you `AZURE_TACKLE.json` file. Example:

```json
{
    "StorageConnectionString": "DefaultEndpointsProtocol=https...."
}
```

### Build Targets

- `Clean` - Cleans artifact and temp directories.
- `DotnetRestore` - Runs [dotnet restore](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-restore?tabs=netcore2x) on the [solution file](https://docs.microsoft.com/en-us/visualstudio/extensibility/internals/solution-dot-sln-file?view=vs-2019).
- [`DotnetBuild`](#Building) - Runs [dotnet build](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-build?tabs=netcore2x) on the [solution file](https://docs.microsoft.com/en-us/visualstudio/extensibility/internals/solution-dot-sln-file?view=vs-2019).
- `DotnetTest` - Runs [dotnet test](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-test?tabs=netcore21) on the [solution file](https://docs.microsoft.com/en-us/visualstudio/extensibility/internals/solution-dot-sln-file?view=vs-2019).
- `GenerateCoverageReport` - Code coverage is run during `DotnetTest` and this generates a report via [ReportGenerator](https://github.com/danielpalme/ReportGenerator).
- `WatchTests` - Runs [dotnet watch](https://docs.microsoft.com/en-us/aspnet/core/tutorials/dotnet-watch?view=aspnetcore-3.0) with the test projects. Useful for rapid feedback loops.
- `GenerateAssemblyInfo` - Generates [AssemblyInfo](https://docs.microsoft.com/en-us/dotnet/api/microsoft.visualbasic.applicationservices.assemblyinfo?view=netframework-4.8) for libraries.
- `DotnetPack` - Runs [dotnet pack](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-pack). This includes running [Source Link](https://github.com/dotnet/sourcelink).
- `PublishToNuGet` - Publishes the NuGet packages generated in `DotnetPack` to NuGet via [paket push](https://fsprojects.github.io/Paket/paket-push.html).
- `BuildDocs` - Generates Documentation from `docsSrc` and the [XML Documentation Comments](https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/xmldoc/) from your libraries in `src`.
- `WatchDocs` - Generates documentation and starts a webserver locally.  It will rebuild and hot reload if it detects any changes made to `docsSrc` files, libraries in `src`, or the `docsTool` itself.
- `ReleaseDocs` - Will stage, commit, and push docs generated in the `BuildDocs` target.
---
