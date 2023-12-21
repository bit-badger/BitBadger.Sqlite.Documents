namespace BitBadger.Sqlite.Documents

open System.Runtime.CompilerServices
open Microsoft.Data.Sqlite

/// Document extensions
[<Extension>]
type SqliteConnectionExtensions =
    
    /// Insert a new document
    [<Extension>]
    static member inline Insert<'TDoc>(conn: SqliteConnection, tableName: string, document: 'TDoc) =
        Document.WithConn.Insert<'TDoc>(tableName, document, conn)

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    [<Extension>]
    static member inline Save<'TDoc>(conn: SqliteConnection, tableName: string, document: 'TDoc) =
        Document.WithConn.Save<'TDoc>(tableName, document, conn)

    /// Count all documents in a table
    [<Extension>]
    static member inline CountAll(conn: SqliteConnection, tableName: string) =
        Document.WithConn.Count.All(tableName, conn)
    
    /// Count matching documents using a text comparison on a JSON field
    [<Extension>]
    static member inline CountByFieldEquals(conn: SqliteConnection, tableName: string, fieldName: string, value: obj) =
        Document.WithConn.Count.ByFieldEquals(tableName, fieldName, value, conn)

    /// Retrieve all documents in the given table
    [<Extension>]
    static member inline FindAll<'TDoc>(conn: SqliteConnection, tableName: string) =
        Document.WithConn.Find.All<'TDoc>(tableName, conn)

    /// Retrieve a document by its ID
    [<Extension>]
    static member inline FindById<'TKey, 'TDoc when 'TDoc: null>(
            conn: SqliteConnection,
            tableName: string,
            docId: 'TKey
        ) =
        Document.WithConn.Find.ById<'TKey, 'TDoc>(tableName, docId, conn)

    /// Execute a text comparison on a JSON field query
    [<Extension>]
    static member inline FindByFieldEquals<'TDoc>(
            conn: SqliteConnection,
            tableName: string,
            fieldName: string,
            value: obj
        ) =
        Document.WithConn.Find.ByFieldEquals<'TDoc>(tableName, fieldName, value, conn)

    /// Execute a text comparison on a JSON field query, returning only the first result
    [<Extension>]
    static member inline FindFirstByFieldEquals<'TDoc when 'TDoc: null>(
            conn: SqliteConnection,
            tableName: string,
            fieldName: string,
            value: obj
        ) =
        Document.WithConn.Find.FirstByFieldEquals<'TDoc>(tableName, fieldName, value, conn)

    /// Update an entire document
    [<Extension>]
    static member inline UpdateFull<'TKey, 'TDoc>(
            conn: SqliteConnection,
            tableName: string,
            docId: 'TKey,
            document: 'TDoc
        ) =
        Document.WithConn.Update.Full(tableName, docId, document, conn)
    
    /// Update an entire document
    [<Extension>]
    static member inline UpdateFullFunc<'TKey, 'TDoc>(
            conn: SqliteConnection,
            tableName: string,
            idFunc: System.Func<'TDoc, 'TKey>,
            document: 'TDoc
        ) =
        Document.WithConn.Update.FullFunc(tableName, idFunc, document, conn)
    
    /// Update a partial document
    [<Extension>]
    static member inline UpdatePartialById<'TKey, 'TPatch>(
            conn: SqliteConnection,
            tableName: string,
            docId: 'TKey,
            partial: 'TPatch
        ) =
        Document.WithConn.Update.PartialById(tableName, docId, partial, conn)
    
    /// Update partial documents using a text comparison on a JSON field
    [<Extension>]
    static member inline UpdatePartialByFieldEquals<'TPatch>(
            conn: SqliteConnection,
            tableName: string,
            fieldName: string,
            value: obj,
            partial: 'TPatch
        ) =
        Document.WithConn.Update.PartialByFieldEquals(tableName, fieldName, value, partial, conn)

    /// Delete a document by its ID
    [<Extension>]
    static member inline DeleteById<'TKey>(conn: SqliteConnection, tableName: string, docId: 'TKey) =
        Document.WithConn.Delete.ById(tableName, docId, conn)

    /// Delete documents by matching a text comparison on a JSON field
    [<Extension>]
    static member inline DeleteByFieldEquals(conn: SqliteConnection, tableName: string, fieldName: string, value: obj) =
        Document.WithConn.Delete.ByFieldEquals(tableName, fieldName, value, conn)

    /// Execute a query that returns a list of results
    [<Extension>]
    static member inline CustomList<'TDoc>(
            conn: SqliteConnection,
            query: string,
            parameters: SqliteParameter seq,
            mapFunc: System.Func<SqliteDataReader, 'TDoc>
        ) =
        Document.WithConn.Custom.List<'TDoc>(query, parameters, mapFunc, conn)

    /// Execute a query that returns one or no results
    [<Extension>]
    static member inline CustomSingle<'TDoc when 'TDoc: null>(
            conn: SqliteConnection,
            query: string,
            parameters: SqliteParameter seq,
            mapFunc: System.Func<SqliteDataReader, 'TDoc>
        ) =
        Document.WithConn.Custom.Single<'TDoc>(query, parameters, mapFunc, conn)
    
    /// Execute a query that does not return a value
    [<Extension>]
    static member inline CustomNonQuery(conn: SqliteConnection, query: string, parameters: SqliteParameter seq) =
        Document.WithConn.Custom.NonQuery(query, parameters, conn)

    /// Execute a query that returns a scalar value
    [<Extension>]
    static member inline CustomScalar<'T when 'T: struct>(
            conn: SqliteConnection,
            query: string,
            parameters: SqliteParameter seq,
            mapFunc: System.Func<SqliteDataReader, 'T>
        ) =
        Document.WithConn.Custom.Scalar<'T>(query, parameters, mapFunc, conn)
