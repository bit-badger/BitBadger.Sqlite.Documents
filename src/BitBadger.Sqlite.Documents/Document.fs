module BitBadger.Sqlite.Documents.Document

open System
open Microsoft.Data.Sqlite

// Alias for F# document module
module FS = BitBadger.Sqlite.FSharp.Documents

/// Add a parameter to a SQLite command, ignoring the return value (can still be accessed on cmd via indexing)
let AddParam(cmd: SqliteCommand, name: string, value: obj) =
    FS.addParam cmd name value
    
let AddIdParam<'TKey>(cmd: SqliteCommand, key: 'TKey) =
    FS.addIdParam cmd key

/// Add a JSON document parameter to a command
let AddJsonParam<'TJson>(cmd: SqliteCommand, name: string, it: 'TJson) =
    FS.addJsonParam cmd name it

/// Add ID (@id) and document (@data) parameters to a command
let AddIdAndDocParams<'TKey, 'TDoc>(cmd: SqliteCommand, docId: 'TKey, doc: 'TDoc) =
    FS.addIdAndDocParams cmd docId doc

/// Create a domain item from a document, specifying the field in which the document is found
let FromDocument<'TDoc>(field: string, rdr: SqliteDataReader) : 'TDoc =
    FS.fromDocument field rdr
    
/// Create a domain item from a document
let FromData<'TDoc>(rdr: SqliteDataReader) : 'TDoc =
    FS.fromData rdr

/// Create a list of items for the results of the given command, using the specified mapping function
let ToCustomList<'TDoc>(cmd: SqliteCommand, mapFunc: Func<SqliteDataReader, 'TDoc>) = backgroundTask {
    let! results = FS.toCustomList<'TDoc> cmd mapFunc.Invoke
    return ResizeArray results
}

/// Create a list of items for the results of the given command
let ToDocumentList<'TDoc>(cmd: SqliteCommand) = backgroundTask {
    let! results = FS.toDocumentList<'TDoc> cmd
    return ResizeArray results
}


/// Versions of queries that accept a SqliteConnection as the last parameter
module WithConn =
    
    /// Insert a new document
    let Insert<'TDoc>(tableName: string, document: 'TDoc, conn: SqliteConnection) =
        FS.WithConn.insert tableName document conn

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    let Save<'TDoc>(tableName: string, document: 'TDoc, conn: SqliteConnection) =
        FS.WithConn.save tableName document conn

    /// Commands to count documents
    [<RequireQualifiedAccess>]
    module Count =
        
        /// Count all documents in a table
        let All(tableName: string, conn: SqliteConnection) =
            FS.WithConn.Count.all tableName conn
        
        /// Count matching documents using a text comparison on a JSON field
        let ByFieldEquals(tableName: string, fieldName: string, value: obj, conn: SqliteConnection) =
            FS.WithConn.Count.byFieldEquals tableName fieldName value conn

    /// Commands to determine if documents exist
    [<RequireQualifiedAccess>]
    module Exists =

        /// Determine if a document exists for the given ID
        let ById<'TKey>(tableName: string, docId: 'TKey, conn: SqliteConnection) =
            FS.WithConn.Exists.byId tableName docId conn

        /// Determine if a document exists using a text comparison on a JSON field
        let ByFieldEquals(tableName: string, fieldName: string, value: obj, conn: SqliteConnection) =
            FS.WithConn.Exists.byFieldEquals tableName fieldName value conn
    
    /// Commands to determine if documents exist
    [<RequireQualifiedAccess>]
    module Find =
        
        /// Retrieve all documents in the given table
        let All<'TDoc>(tableName: string, conn: SqliteConnection) = backgroundTask {
            let! results = FS.WithConn.Find.all<'TDoc> tableName conn
            return ResizeArray results
        }

        /// Retrieve a document by its ID
        let ById<'TKey, 'TDoc when 'TDoc: null>(
                tableName: string,
                docId: 'TKey,
                conn: SqliteConnection
            ) = backgroundTask {
            let! result = FS.WithConn.Find.byId<'TKey, 'TDoc> tableName docId conn
            return Option.toObj result
        }

        /// Execute a text comparison on a JSON field query
        let ByFieldEquals<'TDoc>(
                tableName: string,
                fieldName: string,
                value: obj,
                conn: SqliteConnection
            ) = backgroundTask {
            let! results = FS.WithConn.Find.byFieldEquals<'TDoc> tableName fieldName value conn
            return ResizeArray results
        }

        /// Execute a text comparison on a JSON field query, returning only the first result
        let FirstByFieldEquals<'TDoc when 'TDoc: null>(
                tableName: string,
                fieldName: string,
                value: obj,
                conn: SqliteConnection
            ) = backgroundTask {
            let! result = FS.WithConn.Find.firstByFieldEquals<'TDoc> tableName fieldName value conn
            return Option.toObj result
        }

    /// Commands to update documents
    [<RequireQualifiedAccess>]
    module Update =
        
        /// Update an entire document
        let Full<'TKey, 'TDoc>(tableName: string, docId: 'TKey, document: 'TDoc, conn: SqliteConnection) =
            FS.WithConn.Update.full tableName docId document conn
        
        /// Update an entire document
        let FullFunc<'TKey, 'TDoc>(
                tableName: string,
                idFunc: Func<'TDoc, 'TKey>,
                document: 'TDoc,
                conn: SqliteConnection
            ) =
            FS.WithConn.Update.fullFunc tableName idFunc.Invoke document conn
        
        /// Update a partial document
        let PartialById<'TKey, 'TPatch>(tableName: string, docId: 'TKey, partial: 'TPatch, conn: SqliteConnection) =
            FS.WithConn.Update.partialById tableName docId partial conn
        
        /// Update partial documents using a text comparison on a JSON field
        let PartialByFieldEquals<'TPatch>(
                tableName: string,
                fieldName: string,
                value: obj,
                partial: 'TPatch,
                conn: SqliteConnection
            ) =
            FS.WithConn.Update.partialByFieldEquals tableName fieldName value partial conn

    /// Commands to delete documents
    [<RequireQualifiedAccess>]
    module Delete =
        
        /// Delete a document by its ID
        let ById<'TKey>(tableName: string, docId: 'TKey, conn: SqliteConnection) =
            FS.WithConn.Delete.byId tableName docId conn

        /// Delete documents by matching a text comparison on a JSON field
        let ByFieldEquals(tableName: string, fieldName: string, value: obj, conn: SqliteConnection) =
            FS.WithConn.Delete.byFieldEquals tableName fieldName value conn

    /// Commands to execute custom SQL queries
    [<RequireQualifiedAccess>]
    module Custom =

        /// Execute a query that returns a list of results
        let List<'TDoc>(
                query: string,
                parameters: SqliteParameter seq,
                mapFunc: Func<SqliteDataReader, 'TDoc>,
                conn: SqliteConnection
            ) = backgroundTask {
            let! results = FS.WithConn.Custom.list<'TDoc> query parameters mapFunc.Invoke conn
            return ResizeArray results
        }

        /// Execute a query that returns one or no results
        let Single<'TDoc when 'TDoc: null>(
                query: string,
                parameters: SqliteParameter seq,
                mapFunc: Func<SqliteDataReader, 'TDoc>,
                conn: SqliteConnection
            ) = backgroundTask {
            let! result = FS.WithConn.Custom.single<'TDoc> query parameters mapFunc.Invoke conn
            return Option.toObj result
        }
        
        /// Execute a query that does not return a value
        let NonQuery(query: string, parameters: SqliteParameter seq, conn: SqliteConnection) =
            FS.WithConn.Custom.nonQuery query parameters conn

        /// Execute a query that returns a scalar value
        let Scalar<'T when 'T: struct>(
                query: string,
                parameters: SqliteParameter seq,
                mapFunc: Func<SqliteDataReader, 'T>,
                conn: SqliteConnection
            ) =
            FS.WithConn.Custom.scalar<'T> query parameters mapFunc.Invoke conn

/// Create a new SQLite connection; caller is responsible for disposing
let internal newDbConn () = backgroundTask {
    let conn = FS.Configuration.dbConn ()
    do! conn.OpenAsync()
    return conn
}

/// Insert a new document
let Insert<'TDoc>(tableName: string, document: 'TDoc) = backgroundTask {
    use! conn = newDbConn ()
    do! WithConn.Insert(tableName, document, conn)
}

/// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
let Save<'TDoc>(tableName: string, document: 'TDoc) = backgroundTask {
    use! conn = newDbConn ()
    do! WithConn.Save(tableName, document, conn)
}

/// Commands to count documents
[<RequireQualifiedAccess>]
module Count =
    
    /// Count all documents in a table
    let All(tableName: string) = backgroundTask {
        use! conn = newDbConn ()
        return! WithConn.Count.All(tableName, conn)
    }
    
    /// Count matching documents using a text comparison on a JSON field
    let ByFieldEquals(tableName: string, fieldName: string, value: obj) = backgroundTask {
        use! conn = newDbConn ()
        return! WithConn.Count.ByFieldEquals(tableName, fieldName, value, conn)
    }

/// Commands to determine if documents exist
[<RequireQualifiedAccess>]
module Exists =

    /// Determine if a document exists for the given ID
    let ById<'TKey>(tableName: string, docId: 'TKey) = backgroundTask {
        use! conn = newDbConn ()
        return! WithConn.Exists.ById(tableName, docId, conn)
    }

    /// Determine if a document exists using a text comparison on a JSON field
    let ByFieldEquals(tableName: string, fieldName: string, value: obj) = backgroundTask {
        use! conn = newDbConn ()
        return! WithConn.Exists.ByFieldEquals(tableName, fieldName, value, conn)
    }

/// Commands to determine if documents exist
[<RequireQualifiedAccess>]
module Find =
    
    /// Retrieve all documents in the given table
    let All<'TDoc>(tableName: string) = backgroundTask {
        use! conn = newDbConn ()
        return! WithConn.Find.All<'TDoc>(tableName, conn)
    }

    /// Retrieve a document by its ID
    let ById<'TKey, 'TDoc when 'TDoc: null>(tableName: string, docId: 'TKey) = backgroundTask {
        use! conn = newDbConn ()
        return! WithConn.Find.ById<'TKey, 'TDoc>(tableName, docId, conn)
    }

    /// Execute a text comparison on a JSON field query
    let ByFieldEquals<'TDoc>(tableName: string, fieldName: string, value: obj) = backgroundTask {
        use! conn = newDbConn ()
        return! WithConn.Find.ByFieldEquals<'TDoc>(tableName, fieldName, value, conn)
    }

    /// Execute a text comparison on a JSON field query, returning only the first result
    let FirstByFieldEquals<'TDoc when 'TDoc: null>(tableName: string, fieldName: string, value: obj) = backgroundTask {
        use! conn = newDbConn ()
        return! WithConn.Find.FirstByFieldEquals<'TDoc>(tableName, fieldName, value, conn)
    }

/// Commands to update documents
[<RequireQualifiedAccess>]
module Update =
    
    /// Update an entire document
    let Full<'TKey, 'TDoc>(tableName: string, docId: 'TKey, document: 'TDoc) = backgroundTask {
        use! conn = newDbConn ()
        do! WithConn.Update.Full(tableName, docId, document, conn)
    }
    
    /// Update an entire document
    let FullFunc<'TKey, 'TDoc>(tableName: string, idFunc: Func<'TDoc, 'TKey>, document: 'TDoc) = backgroundTask {
        use! conn = newDbConn ()
        do! WithConn.Update.FullFunc(tableName, idFunc, document, conn)
    }
    
    /// Update a partial document
    let PartialById<'TKey, 'TPatch>(tableName: string, docId: 'TKey, partial: 'TPatch) = backgroundTask {
        use! conn = newDbConn ()
        do! WithConn.Update.PartialById(tableName, docId, partial, conn)
    }
    
    /// Update partial documents using a text comparison on a JSON field in the WHERE clause
    let PartialByFieldEquals<'TPatch>(
            tableName: string,
            fieldName: string,
            value: obj,
            partial: 'TPatch
        ) = backgroundTask {
        use! conn = newDbConn ()
        do! WithConn.Update.PartialByFieldEquals(tableName, fieldName, value, partial, conn)
    }

/// Commands to delete documents
[<RequireQualifiedAccess>]
module Delete =
    
    /// Delete a document by its ID
    let ById<'TKey>(tableName: string, docId: 'TKey) = backgroundTask {
        use! conn = newDbConn ()
        do! WithConn.Delete.ById(tableName, docId, conn)
    }

    /// Delete documents by matching a text comparison on a JSON field
    let ByFieldEquals(tableName: string, fieldName: string, value: obj) = backgroundTask {
        use! conn = newDbConn ()
        do! WithConn.Delete.ByFieldEquals(tableName, fieldName, value, conn)
    }

/// Commands to execute custom SQL queries
[<RequireQualifiedAccess>]
module Custom =

    /// Execute a query that returns a list of results
    let List<'TDoc>(
            query: string,
            parameters: SqliteParameter seq,
            mapFunc: Func<SqliteDataReader, 'TDoc>
        ) = backgroundTask {
        use! conn = newDbConn ()
        return! WithConn.Custom.List<'TDoc>(query, parameters, mapFunc, conn)
    }

    /// Execute a query that returns one or no results
    let Single<'TDoc when 'TDoc: null>(
            query: string,
            parameters: SqliteParameter seq,
            mapFunc: Func<SqliteDataReader, 'TDoc>
        ) = backgroundTask {
        use! conn = newDbConn ()
        return! WithConn.Custom.Single<'TDoc>(query, parameters, mapFunc, conn)
    }

    /// Execute a query that does not return a value
    let NonQuery(query: string, parameters: SqliteParameter seq) = backgroundTask {
        use! conn = newDbConn ()
        return! WithConn.Custom.NonQuery(query, parameters, conn)
    }
    
    /// Execute a query that returns a scalar value
    let Scalar<'T when 'T: struct>(
            query: string,
            parameters: SqliteParameter seq,
            mapFunc: Func<SqliteDataReader, 'T>
        ) = backgroundTask {
        use! conn = newDbConn ()
        return! WithConn.Custom.Scalar<'T>(query, parameters, mapFunc, conn)
    }
