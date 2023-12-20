module BitBadger.Sqlite.Documents.Definition

open Microsoft.Data.Sqlite

// Alias for F# document module
module FS = BitBadger.Sqlite.FSharp.Documents

/// SQL statement to create a document table
let CreateTable(name: string) =
    FS.Definition.createTable name

/// SQL statement to create a key index for a document table
let CreateKey(name: string) =
    FS.Definition.createKey name
    
/// Definitions that take a SqliteConnection as their last parameter
module WithConn =
    
    /// Create a document table
    let EnsureTable(name: string, conn: SqliteConnection) =
        FS.Definition.WithConn.ensureTable name conn

/// Create a document table
let EnsureTable(name: string) =
    FS.Definition.ensureTable name
