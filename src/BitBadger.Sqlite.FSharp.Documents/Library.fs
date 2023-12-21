module BitBadger.Sqlite.FSharp.Documents

open Microsoft.Data.Sqlite

/// Configuration for document handling
module Configuration =

    open System.Text.Json
    open System.Text.Json.Serialization
    open BitBadger.Sqlite.Documents

    /// The default JSON serializer options to use with the stock serializer
    let private jsonDefaultOpts =
        let o = JsonSerializerOptions()
        o.Converters.Add(JsonFSharpConverter())
        o
    
    /// The default JSON serializer
    let internal defaultSerializer =
        { new IDocumentSerializer with
            member _.Serialize<'T>(it: 'T) : string =
                JsonSerializer.Serialize(it, jsonDefaultOpts)
            member _.Deserialize<'T>(it: string) : 'T =
                JsonSerializer.Deserialize<'T>(it, jsonDefaultOpts)
        }
    
    /// The serializer to use for document manipulation
    let mutable private serializerValue = defaultSerializer
    
    /// Register a serializer to use for translating documents to domain types
    let useSerializer ser =
        serializerValue <- ser

    /// Retrieve the currently configured serializer
    let serializer () =
        serializerValue
    
    /// The connection string to use for query execution
    let mutable internal connectionString: string option = None

    /// Register a connection string to use for query execution (enables foreign keys)
    let useConnectionString connStr =
        let builder = SqliteConnectionStringBuilder(connStr)
        builder.ForeignKeys <- Option.toNullable (Some true)
        connectionString <- Some (string builder)
    
    /// Retrieve the currently configured data source
    let dbConn () =
        match connectionString with
        | Some connStr ->
            let conn = new SqliteConnection(connStr)
            conn.Open()
            conn
        | None -> invalidOp "Please provide a connection string before attempting data access"
    
    /// The serialized name of the ID field for documents
    let mutable idFieldValue = "Id"
    
    /// Specify the name of the ID field for documents
    let useIdField it =
        idFieldValue <- it
    
    /// Retrieve the currently configured ID field for documents
    let idField () =
        idFieldValue

/// Create a new SQLite connection; caller is responsible for disposing
let internal newDbConn () = backgroundTask {
    let conn = Configuration.dbConn ()
    do! conn.OpenAsync()
    return conn
}

/// Execute a non-query command
let internal write (cmd: SqliteCommand) = backgroundTask {
    let! _ = cmd.ExecuteNonQueryAsync()
    ()
}

/// Data definition
[<RequireQualifiedAccess>]
module Definition =

    /// SQL statement to create a document table
    let createTable name =
        $"CREATE TABLE IF NOT EXISTS %s{name} (data TEXT NOT NULL)"
    
    /// SQL statement to create a key index for a document table
    let createKey name =
        $"CREATE UNIQUE INDEX IF NOT EXISTS idx_%s{name}_key ON {name} ((data ->> '{Configuration.idField ()}'))"
        
    /// Definitions that take a SqliteConnection as their last parameter
    module WithConn =
        
        /// Create a document table
        let ensureTable name (conn: SqliteConnection) = backgroundTask {
            use cmd = conn.CreateCommand()
            cmd.CommandText <- createTable name
            do! write cmd
            cmd.CommandText <- createKey name
            do! write cmd
        }
    
    /// Create a document table
    let ensureTable name =
        use conn = Configuration.dbConn ()
        WithConn.ensureTable name conn

/// Query construction functions
[<RequireQualifiedAccess>]
module Query =
    
    /// Create a SELECT clause to retrieve the document data from the given table
    let selectFromTable tableName =
        $"SELECT data FROM %s{tableName}"
    
    /// Create a WHERE clause fragment to implement an ID-based query
    let whereById paramName =
        $"data ->> '{Configuration.idField ()}' = %s{paramName}"
    
    /// Create a WHERE clause fragment to implement a text equality check on a field in a JSON document
    let whereFieldEquals fieldName paramName =
        $"data ->> '%s{fieldName}' = %s{paramName}"
    
    /// Query to insert a document
    let insert tableName =
        $"INSERT INTO %s{tableName} VALUES (@data)"

    /// Query to save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    let save tableName =
        sprintf
            "INSERT INTO %s VALUES (@data) ON CONFLICT ((data ->> '%s')) DO UPDATE SET data = EXCLUDED.data"
            tableName (Configuration.idField ()) 
    
    /// Queries for counting documents
    module Count =
        
        /// Query to count all documents in a table
        let all tableName =
            $"SELECT COUNT(*) AS it FROM %s{tableName}"
        
        /// Query to count matching documents using a text comparison on a JSON field
        let byFieldEquals tableName fieldName =
            $"""SELECT COUNT(*) AS it FROM %s{tableName} WHERE {whereFieldEquals fieldName "@field"}"""
        
    /// Queries for determining document existence
    module Exists =

        /// Query to determine if a document exists for the given ID
        let byId tableName =
            $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {whereById "@id"}) AS it"""

        /// Query to determine if documents exist using a text comparison on a JSON field
        let byFieldEquals tableName fieldName =
            $"""SELECT EXISTS (SELECT 1 FROM %s{tableName} WHERE {whereFieldEquals fieldName "@field"}) AS it"""
        
    /// Queries for retrieving documents
    module Find =

        /// Query to retrieve a document by its ID
        let byId tableName =
            $"""{selectFromTable tableName} WHERE {whereById "@id"}"""
        
        /// Query to retrieve documents using a text comparison on a JSON field
        let byFieldEquals tableName fieldName =
            $"""{selectFromTable tableName} WHERE {whereFieldEquals fieldName "@field"}"""
        
    /// Queries to update documents
    module Update =

        /// Query to update a document
        let full tableName =
            $"""UPDATE %s{tableName} SET data = @data WHERE {whereById "@id"}"""

        /// Query to update a partial document by its ID
        let partialById tableName =
            $"""UPDATE %s{tableName} SET data = json_patch(data, json(@data)) WHERE {whereById "@id"}"""
            
        /// Query to update a partial document via a text comparison on a JSON field
        let partialByFieldEquals tableName fieldName =
            sprintf
                "UPDATE %s SET data = json_patch(data, json(@data)) WHERE %s"
                tableName (whereFieldEquals fieldName "@field")
    
    /// Queries to delete documents
    module Delete =
        
        /// Query to delete a document by its ID
        let byId tableName =
            $"""DELETE FROM %s{tableName} WHERE {whereById "@id"}"""

        /// Query to delete documents using a text comparison on a JSON field
        let byFieldEquals tableName fieldName =
            $"""DELETE FROM %s{tableName} WHERE {whereFieldEquals fieldName "@field"}"""


/// Add a parameter to a SQLite command, ignoring the return value (can still be accessed on cmd via indexing)
let addParam (cmd: SqliteCommand) name (value: obj) =
    cmd.Parameters.AddWithValue(name, value) |> ignore
    
let addIdParam (cmd: SqliteCommand) (key: 'TKey) =
    addParam cmd "@id" (string key)

/// Add a JSON document parameter to a command
let addJsonParam (cmd: SqliteCommand) name (it: 'TJson) =
    addParam cmd name (Configuration.serializer().Serialize it)

/// Add ID (@id) and document (@data) parameters to a command
let addIdAndDocParams cmd (docId: 'TKey) (doc: 'TDoc) =
    addIdParam cmd docId
    addJsonParam cmd "@data" doc

/// Create a domain item from a document, specifying the field in which the document is found
let fromDocument<'TDoc> field (rdr: SqliteDataReader) : 'TDoc =
    Configuration.serializer().Deserialize<'TDoc>(rdr.GetString(rdr.GetOrdinal(field)))
    
/// Create a domain item from a document
let fromData<'TDoc> rdr =
    fromDocument<'TDoc> "data" rdr

/// Create a list of items for the results of the given command, using the specified mapping function
let toCustomList<'TDoc> (cmd: SqliteCommand) (mapFunc: SqliteDataReader -> 'TDoc) = backgroundTask {
    use! rdr = cmd.ExecuteReaderAsync()
    let mutable it = Seq.empty<'TDoc>
    while! rdr.ReadAsync() do
        it <- Seq.append it (Seq.singleton (mapFunc rdr))
    return List.ofSeq it
}

/// Create a list of items for the results of the given command
let toDocumentList<'TDoc> (cmd: SqliteCommand) =
    toCustomList<'TDoc> cmd fromData

/// Execute a non-query statement to manipulate a document
let private executeNonQuery query (document: 'T) (conn: SqliteConnection) =
    use cmd = conn.CreateCommand()
    cmd.CommandText <- query
    addJsonParam cmd "@data" document
    write cmd

/// Execute a non-query statement to manipulate a document with an ID specified
let private executeNonQueryWithId query (docId: 'TKey) (document: 'TDoc) (conn: SqliteConnection) =
    use cmd = conn.CreateCommand()
    cmd.CommandText <- query
    addIdAndDocParams cmd docId document
    write cmd


open System.Threading.Tasks

/// Versions of queries that accept a SqliteConnection as the last parameter
module WithConn =
    
    /// Insert a new document
    let insert<'TDoc> tableName (document: 'TDoc) conn =
        executeNonQuery (Query.insert tableName) document conn

    /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
    let save<'TDoc> tableName (document: 'TDoc) conn =
        executeNonQuery (Query.save tableName) document conn

    /// Commands to count documents
    [<RequireQualifiedAccess>]
    module Count =
        
        /// Count all documents in a table
        let all tableName (conn: SqliteConnection) : Task<int64> = backgroundTask {
            use cmd = conn.CreateCommand()
            cmd.CommandText <- Query.Count.all tableName
            let! result = cmd.ExecuteScalarAsync()
            return result :?> int64
        }
        
        /// Count matching documents using a text comparison on a JSON field
        let byFieldEquals tableName fieldName (value: obj) (conn: SqliteConnection) : Task<int64> = backgroundTask {
            use cmd = conn.CreateCommand()
            cmd.CommandText <- Query.Count.byFieldEquals tableName fieldName
            addParam cmd "@field" value
            let! result = cmd.ExecuteScalarAsync()
            return result :?> int64
        }

    /// Commands to determine if documents exist
    [<RequireQualifiedAccess>]
    module Exists =

        /// Determine if a document exists for the given ID
        let byId tableName (docId: 'TKey) (conn: SqliteConnection) : Task<bool> = backgroundTask {
            use cmd = conn.CreateCommand()
            cmd.CommandText <- Query.Exists.byId tableName
            addIdParam cmd docId
            let! result = cmd.ExecuteScalarAsync()
            return (result :?> int64) > 0
        }

        /// Determine if a document exists using a text comparison on a JSON field
        let byFieldEquals tableName fieldName (value: obj) (conn: SqliteConnection) : Task<bool> = backgroundTask {
            use cmd = conn.CreateCommand()
            cmd.CommandText <- Query.Exists.byFieldEquals tableName fieldName
            addParam cmd "@field" value
            let! result = cmd.ExecuteScalarAsync()
            return (result :?> int64) > 0
        }
    
    /// Commands to retrieve documents
    [<RequireQualifiedAccess>]
    module Find =
        
        /// Retrieve all documents in the given table
        let all<'TDoc> tableName (conn: SqliteConnection) : Task<'TDoc list> =
            use cmd = conn.CreateCommand()
            cmd.CommandText <- Query.selectFromTable tableName
            toDocumentList<'TDoc> cmd

        /// Retrieve a document by its ID
        let byId<'TKey, 'TDoc> tableName (docId: 'TKey) (conn: SqliteConnection) : Task<'TDoc option> = backgroundTask {
            use cmd = conn.CreateCommand()
            cmd.CommandText <- Query.Find.byId tableName
            addIdParam cmd docId
            let! results = toDocumentList<'TDoc> cmd
            return List.tryHead results
        }

        /// Execute a text comparison on a JSON field query
        let byFieldEquals<'TDoc> tableName fieldName (value: obj) (conn: SqliteConnection) : Task<'TDoc list> =
            use cmd = conn.CreateCommand()
            cmd.CommandText <- Query.Find.byFieldEquals tableName fieldName
            addParam cmd "@field" value
            toDocumentList<'TDoc> cmd

        /// Execute a text comparison on a JSON field query, returning only the first result
        let firstByFieldEquals<'TDoc> tableName fieldName (value: obj) (conn: SqliteConnection)
                : Task<'TDoc option> = backgroundTask {
            use cmd = conn.CreateCommand()
            cmd.CommandText <- $"{Query.Find.byFieldEquals tableName fieldName} LIMIT 1"
            addParam cmd "@field" value
            let! results = toDocumentList<'TDoc> cmd
            return List.tryHead results
        }

    /// Commands to update documents
    [<RequireQualifiedAccess>]
    module Update =
        
        /// Update an entire document
        let full tableName (docId: 'TKey) (document: 'TDoc) conn =
            executeNonQueryWithId (Query.Update.full tableName) docId document conn
        
        /// Update an entire document
        let fullFunc tableName (idFunc: 'TDoc -> 'TKey) (document: 'TDoc) conn =
            full tableName (idFunc document) document conn
        
        /// Update a partial document
        let partialById tableName (docId: 'TKey) (partial: 'TPatch) conn =
            executeNonQueryWithId (Query.Update.partialById tableName) docId partial conn
        
        /// Update partial documents using a JSON containment query in the WHERE clause (@>)
        let partialByFieldEquals tableName fieldName (value: obj) (partial: 'TPatch) (conn: SqliteConnection) =
            use cmd = conn.CreateCommand()
            cmd.CommandText <- Query.Update.partialByFieldEquals tableName fieldName
            addParam cmd "@field" value
            addJsonParam cmd "@data" partial
            write cmd

    /// Commands to delete documents
    [<RequireQualifiedAccess>]
    module Delete =
        
        /// Delete a document by its ID
        let byId tableName (docId: 'TKey) conn =
            executeNonQueryWithId (Query.Delete.byId tableName) docId {||} conn

        /// Delete documents by matching a text comparison on a JSON field
        let byFieldEquals tableName fieldName (value: obj) (conn: SqliteConnection) =
            use cmd = conn.CreateCommand()
            cmd.CommandText <- Query.Delete.byFieldEquals tableName fieldName
            addParam cmd "@field" value
            write cmd

    /// Commands to execute custom SQL queries
    [<RequireQualifiedAccess>]
    module Custom =

        /// Execute a query that returns a list of results
        let list<'TDoc> query (parameters: SqliteParameter seq) (mapFunc: SqliteDataReader -> 'TDoc)
                (conn: SqliteConnection) =
            use cmd = conn.CreateCommand()
            cmd.CommandText <- query
            cmd.Parameters.AddRange parameters
            toCustomList<'TDoc> cmd mapFunc

        /// Execute a query that returns one or no results
        let single<'TDoc> query parameters (mapFunc: SqliteDataReader -> 'TDoc) conn = backgroundTask {
            let! results = list query parameters mapFunc conn
            return List.tryHead results
        }
        
        /// Execute a query that does not return a value
        let nonQuery query (parameters: SqliteParameter seq) (conn: SqliteConnection) =
            use cmd = conn.CreateCommand()
            cmd.CommandText <- query
            cmd.Parameters.AddRange parameters
            write cmd

        /// Execute a query that returns a scalar value
        let scalar<'T when 'T : struct> query (parameters: SqliteParameter seq) (mapFunc: SqliteDataReader -> 'T)
                (conn: SqliteConnection) = backgroundTask {
            use cmd = conn.CreateCommand()
            cmd.CommandText <- query
            cmd.Parameters.AddRange parameters
            use! rdr = cmd.ExecuteReaderAsync()
            let! isFound = rdr.ReadAsync()
            return if isFound then mapFunc rdr else Unchecked.defaultof<'T>
        }

/// Insert a new document
let insert<'TDoc> tableName (document: 'TDoc) =
    use conn = Configuration.dbConn ()
    WithConn.insert tableName document conn

/// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
let save<'TDoc> tableName (document: 'TDoc) =
    use conn = Configuration.dbConn ()
    WithConn.save tableName document conn

/// Commands to count documents
[<RequireQualifiedAccess>]
module Count =
    
    /// Count all documents in a table
    let all tableName =
        use conn = Configuration.dbConn ()
        WithConn.Count.all tableName conn
    
    /// Count matching documents using a text comparison on a JSON field
    let byFieldEquals tableName fieldName (value: obj) =
        use conn = Configuration.dbConn ()
        WithConn.Count.byFieldEquals tableName fieldName value conn

/// Commands to determine if documents exist
[<RequireQualifiedAccess>]
module Exists =

    /// Determine if a document exists for the given ID
    let byId tableName (docId: 'TKey) =
        use conn = Configuration.dbConn ()
        WithConn.Exists.byId tableName docId conn

    /// Determine if a document exists using a text comparison on a JSON field
    let byFieldEquals tableName fieldName (value: obj) =
        use conn = Configuration.dbConn ()
        WithConn.Exists.byFieldEquals tableName fieldName value conn

/// Commands to determine if documents exist
[<RequireQualifiedAccess>]
module Find =
    
    /// Retrieve all documents in the given table
    let all<'TDoc> tableName =
        use conn = Configuration.dbConn ()
        WithConn.Find.all<'TDoc> tableName conn

    /// Retrieve a document by its ID
    let byId<'TKey, 'TDoc> tableName docId =
        use conn = Configuration.dbConn ()
        WithConn.Find.byId<'TKey, 'TDoc> tableName docId conn

    /// Execute a text comparison on a JSON field query
    let byFieldEquals<'TDoc> tableName fieldName value =
        use conn = Configuration.dbConn ()
        WithConn.Find.byFieldEquals<'TDoc> tableName fieldName value conn

    /// Execute a text comparison on a JSON field query, returning only the first result
    let firstByFieldEquals<'TDoc> tableName fieldName value =
        use conn = Configuration.dbConn ()
        WithConn.Find.firstByFieldEquals<'TDoc> tableName fieldName value conn

/// Commands to update documents
[<RequireQualifiedAccess>]
module Update =
    
    /// Update an entire document
    let full tableName (docId: 'TKey) (document: 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.Update.full tableName docId document conn
    
    /// Update an entire document
    let fullFunc tableName (idFunc: 'TDoc -> 'TKey) (document: 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.Update.fullFunc tableName idFunc document conn
    
    /// Update a partial document
    let partialById tableName (docId: 'TKey) (partial: 'TPatch) =
        use conn = Configuration.dbConn ()
        WithConn.Update.partialById tableName docId partial conn
    
    /// Update partial documents using a text comparison on a JSON field in the WHERE clause
    let partialByFieldEquals tableName fieldName (value: obj) (partial: 'TPatch) =
        use conn = Configuration.dbConn ()
        WithConn.Update.partialByFieldEquals tableName fieldName value partial conn

/// Commands to delete documents
[<RequireQualifiedAccess>]
module Delete =
    
    /// Delete a document by its ID
    let byId tableName (docId: 'TKey) =
        use conn = Configuration.dbConn ()
        WithConn.Delete.byId tableName docId conn

    /// Delete documents by matching a text comparison on a JSON field
    let byFieldEquals tableName fieldName (value: obj) =
        use conn = Configuration.dbConn ()
        WithConn.Delete.byFieldEquals tableName fieldName value conn

/// Commands to execute custom SQL queries
[<RequireQualifiedAccess>]
module Custom =

    /// Execute a query that returns a list of results
    let list<'TDoc> query parameters (mapFunc: SqliteDataReader -> 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.Custom.list<'TDoc> query parameters mapFunc conn

    /// Execute a query that returns one or no results
    let single<'TDoc> query parameters (mapFunc: SqliteDataReader -> 'TDoc) =
        use conn = Configuration.dbConn ()
        WithConn.Custom.single<'TDoc> query parameters mapFunc conn

    /// Execute a query that does not return a value
    let nonQuery query parameters =
        use conn = Configuration.dbConn ()
        WithConn.Custom.nonQuery query parameters conn
    
    /// Execute a query that returns a scalar value
    let scalar<'T when 'T : struct> query parameters (mapFunc: SqliteDataReader -> 'T) =
        use conn = Configuration.dbConn ()
        WithConn.Custom.scalar<'T> query parameters mapFunc conn

[<AutoOpen>]
module Extensions =

    type SqliteConnection with
        
        /// Insert a new document
        member conn.insert<'TDoc> tableName (document: 'TDoc) =
            WithConn.insert<'TDoc> tableName document conn

        /// Save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
        member conn.save<'TDoc> tableName (document: 'TDoc) =
            WithConn.save tableName document conn

        /// Count all documents in a table
        member conn.countAll tableName =
            WithConn.Count.all tableName conn
        
        /// Count matching documents using a text comparison on a JSON field
        member conn.countByFieldEquals tableName fieldName (value: obj) =
            WithConn.Count.byFieldEquals tableName fieldName value conn
        
        /// Determine if a document exists for the given ID
        member conn.existsById tableName (docId: 'TKey) =
            WithConn.Exists.byId tableName docId conn

        /// Determine if a document exists using a text comparison on a JSON field
        member conn.existsByFieldEquals tableName fieldName (value: obj) =
            WithConn.Exists.byFieldEquals tableName fieldName value conn

        /// Retrieve all documents in the given table
        member conn.findAll<'TDoc> tableName =
            WithConn.Find.all<'TDoc> tableName conn

        /// Retrieve a document by its ID
        member conn.findById<'TKey, 'TDoc> tableName (docId: 'TKey) =
            WithConn.Find.byId<'TKey, 'TDoc> tableName docId conn

        /// Execute a text comparison on a JSON field query
        member conn.findByFieldEquals<'TDoc> tableName fieldName (value: obj) =
            WithConn.Find.byFieldEquals<'TDoc> tableName fieldName value conn

        /// Execute a text comparison on a JSON field query, returning only the first result
        member conn.findFirstByFieldEquals<'TDoc> tableName fieldName (value: obj) =
            WithConn.Find.firstByFieldEquals<'TDoc> tableName fieldName value conn

        /// Update an entire document
        member conn.updateFull tableName (docId: 'TKey) (document: 'TDoc) =
            WithConn.Update.full tableName docId document conn
        
        /// Update an entire document
        member conn.updateFullFunc tableName (idFunc: 'TDoc -> 'TKey) (document: 'TDoc) =
            WithConn.Update.fullFunc tableName idFunc document conn
        
        /// Update a partial document
        member conn.updatePartialById tableName (docId: 'TKey) (partial: 'TPatch) =
            WithConn.Update.partialById tableName docId partial conn
        
        /// Update partial documents using a JSON containment query in the WHERE clause (@>)
        member conn.updatePartialByFieldEquals tableName fieldName (value: obj) (partial: 'TPatch) =
            WithConn.Update.partialByFieldEquals tableName fieldName value partial conn

        /// Delete a document by its ID
        member conn.deleteById tableName (docId: 'TKey) =
            WithConn.Delete.byId tableName docId conn

        /// Delete documents by matching a text comparison on a JSON field
        member conn.deleteByFieldEquals tableName fieldName (value: obj) =
            WithConn.Delete.byFieldEquals tableName fieldName value conn

        /// Execute a query that returns a list of results
        member conn.customList<'TDoc> query parameters mapFunc =
            WithConn.Custom.list<'TDoc> query parameters mapFunc conn

        /// Execute a query that returns one or no results
        member conn.customSingle<'TDoc> query parameters mapFunc =
            WithConn.Custom.single query parameters mapFunc conn
        
        /// Execute a query that does not return a value
        member conn.customNonQuery query parameters =
            WithConn.Custom.nonQuery query parameters conn

        /// Execute a query that returns a scalar value
        member conn.customScalar<'T when 'T: struct> query parameters mapFunc =
            WithConn.Custom.scalar query parameters mapFunc conn
