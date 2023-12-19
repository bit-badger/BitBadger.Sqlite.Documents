module Db

open System
open System.IO
open System.Threading.Tasks
open BitBadger.Sqlite.FSharp.Documents

/// The name of the table used for testing
let tableName = "test_table"

/// A throwaway SQLite database file, which will be deleted when it goes out of scope
type ThrowawaySqliteDb(dbName: string) =
    let deleteMe () =
        if File.Exists dbName then File.Delete dbName
    interface IDisposable with
        member _.Dispose() =
            deleteMe ()
    interface IAsyncDisposable with
        member _.DisposeAsync() =
            deleteMe ()
            ValueTask.CompletedTask

/// Create a throwaway database file with the test_table defined
let buildDb () = task {
    let dbName = $"""test-db-{Guid.NewGuid().ToString("n")}.db"""
    Configuration.useConnectionString $"data source={dbName}"
    do! Definition.ensureTable tableName
    return new ThrowawaySqliteDb(dbName)
}
