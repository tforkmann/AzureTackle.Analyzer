namespace AzureTackle.Analyzers.Core

open FSharp.Compiler.Symbols
open FSharp.Compiler.Text
open FSharp.Compiler.Syntax

type AzureTableAnalyzerContext =
  { FileName: string
    Content: string[]
    ParseTree: ParsedInput
    Symbols: FSharpEntity list }

type Fix =
  { FromRange: range
    FromText: string
    ToText: string }

type Severity =
  | Info
  | Warning
  | Error

type Message =
  { Type: string
    Message: string
    Code: string
    Severity: Severity
    Range: range
    Fixes: Fix list }

  member self.IsWarning() = self.Severity = Warning
  member self.IsInfo() = self.Severity = Info
  member self.IsError() = self.Severity = Error

type ColumnReadAttempt =
  { funcName: string
    columnName: string
    columnNameRange: range
    funcCallRange: range }

type UsedFilter =
  { name: string
    range: range
    filterFunc: string
    filterFuncRange: range
    applicationRange: range option }

type FilterSet =
  { filters: UsedFilter list
    range: range }

type TransactionQuery =
  { query: string
    queryRange: range
    filterSets: FilterSet list }

[<RequireQualifiedAccess>]
type AzureAnalyzerBlock =
  | Table of string * range
  | ReadingColumns of ColumnReadAttempt list
  | Filters of UsedFilter list * range

type AzureOperation =
  { blocks: AzureAnalyzerBlock list
    range: range }

type AnalysisType =
  | Filter
  | Table
  | Execute
  | Connection
  | Other

  member this.GetValue =
    match this with
    | Filter -> "FilterAnalysis"
    | Table -> "TableAnalysis"
    | Execute -> "ExecuteAnalysis"
    | Connection -> "ConnectionAnalysis"
    | Other -> "OtherAnalysis"
