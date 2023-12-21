namespace BitBadger.Sqlite.Documents

open System.Runtime.CompilerServices
open Microsoft.Data.Sqlite

/// Document extensions
[<Extension>]
type SqliteConnectionExtensions =
    
    /// Create a document table
    [<Extension>]
    static member inline EnsureTable(conn: SqliteConnection, name: string) =
        Definition.WithConn.EnsureTable(name, conn)

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
    
    /// Count matching documents using a comparison on a JSON field
    [<Extension>]
    static member inline CountByField(
            conn: SqliteConnection,
            tableName: string,
            fieldName: string,
            op: Op,
            value: obj) =
        Document.WithConn.Count.ByField(tableName, fieldName, op, value, conn)

    /// Determine if a document exists for the given ID
    [<Extension>]
    static member inline ExistsById<'TKey>(conn: SqliteConnection, tableName: string, docId: 'TKey) =
        Document.WithConn.Exists.ById(tableName, docId, conn)

    /// Determine if a document exists using a comparison on a JSON field
    [<Extension>]
    static member inline ExistsByField(
            conn: SqliteConnection,
            tableName: string,
            fieldName: string,
            op: Op,
            value: obj) =
        Document.WithConn.Exists.ByField(tableName, fieldName, op, value, conn)
    
    /// Retrieve all documents in the given table
    [<Extension>]
    static member inline FindAll<'TDoc>(conn: SqliteConnection, tableName: string) =
        Document.WithConn.Find.All<'TDoc>(tableName, conn)

    /// Retrieve a document by its ID
    [<Extension>]
    static member inline FindById<'TKey, 'TDoc when 'TDoc: null>(
            conn: SqliteConnection,
            tableName: string,
            docId: 'TKey) =
        Document.WithConn.Find.ById<'TKey, 'TDoc>(tableName, docId, conn)

    /// Retrieve documents via a comparison on a JSON field
    [<Extension>]
    static member inline FindByField<'TDoc>(
            conn: SqliteConnection,
            tableName: string,
            fieldName: string,
            op: Op,
            value: obj) =
        Document.WithConn.Find.ByField<'TDoc>(tableName, fieldName, op, value, conn)

    /// Retrieve documents via a comparison on a JSON field, returning only the first result
    [<Extension>]
    static member inline FindFirstByField<'TDoc when 'TDoc: null>(
            conn: SqliteConnection,
            tableName: string,
            fieldName: string,
            op: Op,
            value: obj) =
        Document.WithConn.Find.FirstByField<'TDoc>(tableName, fieldName, op, value, conn)

    /// Update an entire document
    [<Extension>]
    static member inline UpdateFull<'TKey, 'TDoc>(
            conn: SqliteConnection,
            tableName: string,
            docId: 'TKey,
            document: 'TDoc) =
        Document.WithConn.Update.Full(tableName, docId, document, conn)
    
    /// Update an entire document
    [<Extension>]
    static member inline UpdateFullFunc<'TKey, 'TDoc>(
            conn: SqliteConnection,
            tableName: string,
            idFunc: System.Func<'TDoc, 'TKey>,
            document: 'TDoc) =
        Document.WithConn.Update.FullFunc(tableName, idFunc, document, conn)
    
    /// Update a partial document
    [<Extension>]
    static member inline UpdatePartialById<'TKey, 'TPatch>(
            conn: SqliteConnection,
            tableName: string,
            docId: 'TKey,
            partial: 'TPatch) =
        Document.WithConn.Update.PartialById(tableName, docId, partial, conn)
    
    /// Update partial documents using a comparison on a JSON field
    [<Extension>]
    static member inline UpdatePartialByField<'TPatch>(
            conn: SqliteConnection,
            tableName: string,
            fieldName: string,
            op: Op,
            value: obj,
            partial: 'TPatch) =
        Document.WithConn.Update.PartialByField(tableName, fieldName, op, value, partial, conn)

    /// Delete a document by its ID
    [<Extension>]
    static member inline DeleteById<'TKey>(conn: SqliteConnection, tableName: string, docId: 'TKey) =
        Document.WithConn.Delete.ById(tableName, docId, conn)

    /// Delete documents by matching a comparison on a JSON field
    [<Extension>]
    static member inline DeleteByField(
            conn: SqliteConnection,
            tableName: string,
            fieldName: string,
            op: Op,
            value: obj) =
        Document.WithConn.Delete.ByField(tableName, fieldName, op, value, conn)

    /// Execute a query that returns a list of results
    [<Extension>]
    static member inline CustomList<'TDoc>(
            conn: SqliteConnection,
            query: string,
            parameters: SqliteParameter seq,
            mapFunc: System.Func<SqliteDataReader, 'TDoc>) =
        Document.WithConn.Custom.List<'TDoc>(query, parameters, mapFunc, conn)

    /// Execute a query that returns one or no results
    [<Extension>]
    static member inline CustomSingle<'TDoc when 'TDoc: null>(
            conn: SqliteConnection,
            query: string,
            parameters: SqliteParameter seq,
            mapFunc: System.Func<SqliteDataReader, 'TDoc>) =
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
            mapFunc: System.Func<SqliteDataReader, 'T>) =
        Document.WithConn.Custom.Scalar<'T>(query, parameters, mapFunc, conn)
