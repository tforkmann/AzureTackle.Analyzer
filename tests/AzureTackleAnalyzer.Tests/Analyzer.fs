module AnalyzerBootstrap

open System
open System.IO
open FSharp.Compiler.SourceCodeServices
open FSharp.Compiler.Text
open FSharp.Analyzers.SDK
open GlobExpressions
open Ionide.ProjInfo
open Argu

type Arguments =
    | Project of string
    | Analyzers_Path of string
    | Fail_On_Warnings of string list
    | Ignore_Files of string list
    | Verbose
    interface IArgParserTemplate with
        member s.Usage = ""

let mutable verbose = false

let createFCS () =
    let checker =
        FSharpChecker.Create(projectCacheSize = 200, keepAllBackgroundResolutions = true, keepAssemblyContents = true)

    checker.ImplicitlyStartBackgroundWork <- true
    checker

let fcs = createFCS ()

let parser = ArgumentParser.Create<Arguments>()

let rec mkKn (ty: System.Type) =
    if Reflection.FSharpType.IsFunction(ty) then
        let _, ran =
            Reflection.FSharpType.GetFunctionElements(ty)

        let f = mkKn ran
        Reflection.FSharpValue.MakeFunction(ty, (fun _ -> f))
    else
        box ()


let printInfo (fmt: Printf.TextWriterFormat<'a>): 'a =
    if verbose then
        Console.ForegroundColor <- ConsoleColor.DarkGray
        printf "Info : "
        Console.ForegroundColor <- ConsoleColor.White
        printfn fmt
    else
        unbox (mkKn typeof<'a>)

let printError text arg =
    Console.ForegroundColor <- ConsoleColor.Red
    printf "Error : "
    printfn text arg
    Console.ForegroundColor <- ConsoleColor.White

let loadProject projPath toolsPath =
    async {
        let loader = WorkspaceLoader.Create(toolsPath)
        let parsed =
            loader.LoadProjects [ projPath ] |> Seq.toList
        let fcsPo =
            match parsed |> List.tryHead with
            | Some p -> FCS.mapToFSharpProjectOptions p parsed
            | None -> failwithf "The loaded project from path %s could not parse any file" projPath
        return fcsPo
    }
    |> Async.RunSynchronously

let typeCheckFile (file, opts) =
    let text = File.ReadAllText file
    let st = SourceText.ofString text

    let (parseRes, checkAnswer) =
        fcs.ParseAndCheckFileInProject(file, 0, st, opts)
        |> Async.RunSynchronously //ToDo: Validate if 0 is ok

    match checkAnswer with
    | FSharpCheckFileAnswer.Aborted ->
        printError "Checking of file %s aborted" file
        None
    | FSharpCheckFileAnswer.Succeeded result -> Some(file, text, parseRes, result)

let entityCache = EntityCache()

let getAllEntities (checkResults: FSharpCheckFileResults) (publicOnly: bool): AssemblySymbol list =
    try
        let res =
            [ yield!
                AssemblyContentProvider.getAssemblySignatureContent
                    AssemblyContentType.Full
                    checkResults.PartialAssemblySignature
              let ctx = checkResults.ProjectContext

              let assembliesByFileName =
                  ctx.GetReferencedAssemblies()
                  |> Seq.groupBy (fun asm -> asm.FileName)
                  |> Seq.map (fun (fileName, asms) -> fileName, List.ofSeq asms)
                  |> Seq.toList
                  |> List.rev // if mscorlib.dll is the first then FSC raises exception when we try to
              // get Content.Entities from it.

              for fileName, signatures in assembliesByFileName do
                  let contentType = if publicOnly then Public else Full

                  let content =
                      AssemblyContentProvider.getAssemblyContent entityCache.Locking contentType fileName signatures

                  yield! content ]

        res
    with _ -> []

let createContext (file, text: string, p: FSharpParseFileResults, c: FSharpCheckFileResults) =
    match p.ParseTree, c.ImplementationFile with
    | Some pt, Some tast ->
        let context: Context =
            { FileName = file
              Content = text.Split([| '\n' |])
              ParseTree = pt
              TypedTree = tast
              Symbols = c.PartialAssemblySignature.Entities |> Seq.toList
              GetAllEntities = getAllEntities c }

        Some context
    | _ -> None


let getOpts proj toolsPath =
    let pathProj =
        Path.Combine(Environment.CurrentDirectory, proj)
        |> Path.GetFullPath
    loadProject pathProj toolsPath
let runProject (opts:FSharpProjectOptions ) selectedFile (globs: Glob list) =
    let pathFile =
        Path.Combine(Environment.CurrentDirectory, selectedFile)
        |> Path.GetFullPath
    opts.SourceFiles
    |> Array.filter (fun file -> file = pathFile)
    |> Array.filter
        (fun file ->
            match globs |> List.tryFind (fun g -> g.IsMatch file) with
            | Some g ->
                printInfo $"Ignoring file %s{file} for pattern %s{g.Pattern}"
                false
            | None -> true)
    |> Array.choose
        (fun f ->
            typeCheckFile (f, opts)
            |> Option.map createContext)

let printMessages failOnWarnings (msgs: Message array) =
    if verbose then printfn ""

    if verbose && Array.isEmpty msgs
    then printfn "No messages found from the analyzer(s)"

    msgs
    |> Seq.iter
        (fun m ->
            let color =
                match m.Severity with
                | Error -> ConsoleColor.Red
                | Warning when failOnWarnings |> List.contains m.Code -> ConsoleColor.Red
                | Warning -> ConsoleColor.DarkYellow
                | Info -> ConsoleColor.Blue

            Console.ForegroundColor <- color

            printfn
                "%s(%d,%d): %s %s - %s"
                m.Range.FileName
                m.Range.StartLine
                m.Range.StartColumn
                (m.Severity.ToString())
                m.Code
                m.Message

            Console.ForegroundColor <- ConsoleColor.White)

    msgs

let calculateExitCode failOnWarnings (msgs: Message array option): int =
    match msgs with
    | None -> -1
    | Some msgs ->
        let check =
            msgs
            |> Array.exists
                (fun n ->
                    n.Severity = Error
                    || (n.Severity = Warning
                        && failOnWarnings |> List.contains n.Code))

        if check then -2 else 0

let dumpOpts (opts: FSharpProjectOptions) =
    printfn "FSharpProjectOptions.OtherOptions ->"
    opts.OtherOptions |> Array.iter (printfn "%s")

let context opts selectedFile globs =

    match runProject opts selectedFile globs |> Array.tryHead with
    | Some c -> c
    | None -> failwithf "could find any project"
