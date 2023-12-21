/// Query creation utilities for use with the SQLite document store
module BitBadger.Sqlite.Documents.Query

// Alias for F# document module
module FS = BitBadger.Sqlite.FSharp.Documents

/// Create a SELECT clause to retrieve the document data from the given table
let SelectFromTable(tableName: string) =
    FS.Query.selectFromTable tableName

/// Create a WHERE clause fragment to implement a comparison on a field in a JSON document
let WhereByField(fieldName: string, op: Op, paramName: string) =
    FS.Query.whereByField fieldName (Op.convert op) paramName

/// Create a WHERE clause fragment to implement an ID-based query
let WhereById(paramName: string) =
    FS.Query.whereById paramName

/// Query to insert a document
let Insert(tableName: string) =
    FS.Query.insert tableName

/// Query to save a document, inserting it if it does not exist and updating it if it does (AKA "upsert")
let Save(tableName: string) =
    FS.Query.save tableName

/// Queries for counting documents
module Count =
    
    /// Query to count all documents in a table
    let All(tableName: string) =
        FS.Query.Count.all tableName
    
    /// Query to count matching documents using a comparison on a JSON field
    let ByField(tableName: string, fieldName: string, op: Op) =
        FS.Query.Count.byField tableName fieldName (Op.convert op)
    
/// Queries for determining document existence
module Exists =

    /// Query to determine if a document exists for the given ID
    let ById(tableName: string) =
        FS.Query.Exists.byId tableName

    /// Query to determine if documents exist using a comparison on a JSON field
    let ByField(tableName: string, fieldName: string, op: Op) =
        FS.Query.Exists.byField tableName fieldName (Op.convert op)
    
/// Queries for retrieving documents
module Find =

    /// Query to retrieve a document by its ID
    let ById(tableName: string) =
        FS.Query.Find.byId tableName
    
    /// Query to retrieve documents using a comparison on a JSON field
    let ByField(tableName: string, fieldName: string, op: Op) =
        FS.Query.Find.byField tableName fieldName (Op.convert op)
    
/// Queries to update documents
module Update =

    /// Query to update a document
    let Full(tableName: string) =
        FS.Query.Update.full tableName

    /// Query to update a partial document by its ID
    let PartialById(tableName: string) =
        FS.Query.Update.partialById tableName
        
    /// Query to update a partial document via a comparison on a JSON field
    let PartialByField(tableName: string, fieldName: string, op: Op) =
        FS.Query.Update.partialByField tableName fieldName (Op.convert op)

/// Queries to delete documents
module Delete =
    
    /// Query to delete a document by its ID
    let ById(tableName: string) =
        FS.Query.Delete.byId tableName

    /// Query to delete documents using a comparison on a JSON field
    let ByField(tableName: string, fieldName: string, op: Op) =
        FS.Query.Delete.byField tableName fieldName (Op.convert op)
