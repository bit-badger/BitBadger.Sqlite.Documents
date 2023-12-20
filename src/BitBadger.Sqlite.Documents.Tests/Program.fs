open Expecto

// Use a SQLite in-memory transient database by default
BitBadger.Sqlite.FSharp.Documents.Configuration.useConnectionString "Data Source=:memory:"

let allTests = testList "BitBadger.Sqlite" [ FSharpTests.all; CSharpTests.all ]

[<EntryPoint>]
let main args = runTestsWithCLIArgs [] args allTests
