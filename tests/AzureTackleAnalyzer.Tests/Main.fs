module ExpectoTemplate

open Expecto
open Expecto.Logging
open Ionide.ProjInfo
open Expecto.Impl

[<EntryPoint>]
let main argv =
    let toolsPath = Init.init ()
    printfn "ToolsPath %A" toolsPath
    runTests
        { defaultConfig with
              printer = TestPrinters.summaryPrinter defaultConfig.printer
              verbosity = Verbose }
        (Tests.tests toolsPath)
