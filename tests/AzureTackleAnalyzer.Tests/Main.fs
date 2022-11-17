module ExpectoTemplate

open Expecto
open Expecto.Logging
open Ionide.ProjInfo
open Expecto.Impl
open System.IO

[<EntryPoint>]
let main argv =
    let projectDirectory = DirectoryInfo "./"
    let toolsPath = Init.init projectDirectory None

    runTests
        { defaultConfig with
            printer = TestPrinters.summaryPrinter defaultConfig.printer
            verbosity = Verbose
            runInParallel = false }
        (Tests.tests toolsPath)
