module ExpectoTemplate

open Expecto
open Expecto.Logging
open Ionide.ProjInfo
open Expecto.Impl

[<EntryPoint>]
let main argv =
    let toolsPath = Init.init ()
    runTests
        { defaultConfig with
              printer = TestPrinters.summaryPrinter defaultConfig.printer
              verbosity = Verbose
              runInParallel = false }
        (Tests.tests toolsPath)
