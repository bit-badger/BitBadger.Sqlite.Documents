/// Configuration for the SQLite document store
module BitBadger.Sqlite.Documents.Configuration

open BitBadger.Sqlite.FSharp.Documents

/// Register a serializer to use for translating documents to domain types
let UseSerializer(ser: IDocumentSerializer) =
    Configuration.useSerializer ser

/// Retrieve the currently configured serializer
let Serializer() =
    Configuration.serializer ()

/// Register a connection string to use for query execution (enables foreign keys)
let UseConnectionString(connStr: string) =
    Configuration.useConnectionString connStr

/// Retrieve the currently configured data source
let DbConn() =
    Configuration.dbConn ()

/// Specify the name of the ID field for documents
let UseIdField(it: string) =
    Configuration.useIdField it

/// Retrieve the currently configured ID field for documents
let IdField() =
    Configuration.idField ()
