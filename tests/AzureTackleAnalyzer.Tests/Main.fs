module ExpectoTemplate

open Expecto
open Expecto.Logging
open Ionide.ProjInfo
open Expecto.Impl

[<EntryPoint>]
let main argv =
    let toolsPath = Init.init ()
    printfn "toolsPath %A" toolsPath

    Tests.runTests
        { defaultConfig with
              printer = TestPrinters.summaryPrinter defaultConfig.printer
              verbosity = Verbose }
        (Tests.tests toolsPath)
