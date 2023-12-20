module CSharpTests

open BitBadger.Sqlite.Documents
open Expecto
open Microsoft.Data.Sqlite

[<AllowNullLiteral>]
type SubDocument () =
    member val Foo = "" with get, set
    member val Bar = "" with get, set

[<AllowNullLiteral>]
type JsonDocument() =
    member val Id = "" with get, set
    member val Value = "" with get, set
    member val NumValue = 0 with get, set
    member val Sub : SubDocument option = None with get, set

/// Tests which do not hit the database
let unitTests =
    testList "Unit" [
        testList "Definition" [
            test "CreateTable succeeds" {
                Expect.equal (Definition.CreateTable Db.tableName)
                    $"CREATE TABLE IF NOT EXISTS {Db.tableName} (data TEXT NOT NULL)"
                    "CREATE TABLE statement not constructed correctly"
            }
            test "CreateKey succeeds" {
                Expect.equal (Definition.CreateKey Db.tableName)
                    $"CREATE UNIQUE INDEX IF NOT EXISTS idx_{Db.tableName}_key ON {Db.tableName} ((data ->> 'Id'))"
                    "CREATE INDEX for key statement not constructed correctly"
            }
        ]
        testList "Query" [
            test "SelectFromTable succeeds" {
                Expect.equal
                    (Query.SelectFromTable Db.tableName)
                    $"SELECT data FROM {Db.tableName}"
                    "SELECT statement not correct"
            }
            test "WhereById succeeds" {
                Expect.equal (Query.WhereById "@id") "data ->> 'Id' = @id" "WHERE clause not correct"
            }
            test "WhereFieldEquals succeeds" {
                Expect.equal
                    (Query.WhereFieldEquals("theField", "@test"))
                    "data ->> 'theField' = @test"
                    "WHERE clause not correct"
            }
            test "Insert succeeds" {
                Expect.equal
                    (Query.Insert Db.tableName)
                    $"INSERT INTO {Db.tableName} VALUES (@data)"
                    "INSERT statement not correct"
            }
            test "Save succeeds" {
                Expect.equal
                    (Query.Save Db.tableName)
                    $"INSERT INTO {Db.tableName} VALUES (@data) ON CONFLICT ((data ->> 'Id')) DO UPDATE SET data = EXCLUDED.data"
                    "INSERT ON CONFLICT UPDATE statement not correct"
            }
            testList "Count" [
                test "All succeeds" {
                    Expect.equal
                        (Query.Count.All Db.tableName)
                        $"SELECT COUNT(*) AS it FROM {Db.tableName}"
                        "Count query not correct"
                }
                test "ByFieldEquals succeeds" {
                    Expect.equal
                        (Query.Count.ByFieldEquals(Db.tableName, "thatField"))
                        $"SELECT COUNT(*) AS it FROM {Db.tableName} WHERE data ->> 'thatField' = @field"
                        "JSON field text comparison count query not correct"
                }
            ]
            testList "Exists" [
                test "ById succeeds" {
                    Expect.equal
                        (Query.Exists.ById Db.tableName)
                        $"SELECT EXISTS (SELECT 1 FROM {Db.tableName} WHERE data ->> 'Id' = @id) AS it"
                        "ID existence query not correct"
                }
                test "ByFieldEquals succeeds" {
                    Expect.equal
                        (Query.Exists.ByFieldEquals(Db.tableName, "Test"))
                        $"SELECT EXISTS (SELECT 1 FROM {Db.tableName} WHERE data ->> 'Test' = @field) AS it"
                        "JSON field text comparison exists query not correct"
                }
            ]
            testList "Find" [
                test "ById succeeds" {
                    Expect.equal
                        (Query.Find.ById Db.tableName)
                        $"SELECT data FROM {Db.tableName} WHERE data ->> 'Id' = @id"
                        "SELECT by ID query not correct"
                }
                test "ByFieldEquals succeeds" {
                    Expect.equal
                        (Query.Find.ByFieldEquals(Db.tableName, "Golf"))
                        $"SELECT data FROM {Db.tableName} WHERE data ->> 'Golf' = @field"
                        "SELECT by JSON text comparison query not correct"
                }
            ]
            testList "Update" [
                test "Full succeeds" {
                    Expect.equal
                        (Query.Update.Full Db.tableName)
                        $"UPDATE {Db.tableName} SET data = @data WHERE data ->> 'Id' = @id"
                        "UPDATE full statement not correct"
                }
                test "PartialById succeeds" {
                    Expect.equal
                        (Query.Update.PartialById Db.tableName)
                        $"UPDATE {Db.tableName} SET data = json_patch(data, json(@data)) WHERE data ->> 'Id' = @id"
                        "UPDATE partial by ID statement not correct"
                }
                test "PartialByFieldEquals succeeds" {
                    Expect.equal
                        (Query.Update.PartialByFieldEquals(Db.tableName, "Part"))
                        $"UPDATE {Db.tableName} SET data = json_patch(data, json(@data)) WHERE data ->> 'Part' = @field"
                        "UPDATE partial by JSON containment statement not correct"
                }
            ]
            testList "Delete" [
                test "ById succeeds" {
                    Expect.equal
                        (Query.Delete.ById Db.tableName)
                        $"DELETE FROM {Db.tableName} WHERE data ->> 'Id' = @id"
                        "DELETE by ID query not correct"
                }
                test "ByFieldEquals succeeds" {
                    Expect.equal
                        (Query.Delete.ByFieldEquals(Db.tableName, "gone"))
                        $"DELETE FROM {Db.tableName} WHERE data ->> 'gone' = @field"
                        "DELETE by JSON containment query not correct"
                }
            ]
        ]
    ]

let isTrue<'T> (_ : 'T) = true

module FS = BitBadger.Sqlite.FSharp.Documents

open Document

let integrationTests =
    let documents = [
        JsonDocument(Id = "one", Value = "FIRST!", NumValue = 0)
        JsonDocument(Id = "two", Value = "another", NumValue = 10, Sub = Some(SubDocument(Foo = "green", Bar = "blue")))
        JsonDocument(Id = "three", Value = "", NumValue = 4)
        JsonDocument(Id = "four", Value = "purple", NumValue = 17, Sub = Some(SubDocument(Foo = "green", Bar = "red")))
        JsonDocument(Id = "five", Value = "purple", NumValue = 18)
    ]
    let loadDocs () = backgroundTask {
        for doc in documents do do! Insert(Db.tableName, doc)
    }
    testList "Integration" [
        testList "Configuration" [
            test "UseConnectionString succeeds" {
                try
                    Configuration.UseConnectionString "Data Source=test.db"
                    Expect.equal
                        FS.Configuration.connectionString
                        (Some "Data Source=test.db;Foreign Keys=True")
                        "Connection string incorrect"
                finally
                    Configuration.UseConnectionString "Data Source=:memory:"
            }
            test "UseSerializer succeeds" {
                try
                    Configuration.UseSerializer
                        { new IDocumentSerializer with
                            member _.Serialize<'T>(it: 'T) : string = """{"Overridden":true}"""
                            member _.Deserialize<'T>(it: string) : 'T = Unchecked.defaultof<'T>
                        }
                    
                    let serialized = Configuration.Serializer().Serialize(SubDocument(Foo = "howdy", Bar = "bye"))
                    Expect.equal serialized """{"Overridden":true}""" "Specified serializer was not used"
                    
                    let deserialized = Configuration.Serializer().Deserialize<obj> """{"Something":"here"}"""
                    Expect.isNull deserialized "Specified serializer should have returned null"
                finally
                    Configuration.UseSerializer FS.Configuration.defaultSerializer
            }
            test "Serializer returns configured serializer" {
                Expect.isTrue
                    (obj.ReferenceEquals(FS.Configuration.defaultSerializer, Configuration.Serializer()))
                    "Serializer should have been the same"
            }
            test "UseIdField / IdField succeed" {
                Expect.equal (Configuration.IdField()) "Id" "The default configured ID field was incorrect"
                Configuration.UseIdField "id"
                Expect.equal (Configuration.IdField()) "id" "useIdField did not set the ID field"
                Configuration.UseIdField "Id"
            }
        ]
        testList "Definition" [
            testTask "EnsureTable succeeds" {
                use! db = Db.buildDb ()
                let itExists (name: string) = task {
                    let! result =
                        Custom.Scalar(
                            $"SELECT EXISTS (SELECT 1 FROM {Db.catalog} WHERE name = @name) AS it",
                            [ SqliteParameter("@name", name) ],
                            System.Func<SqliteDataReader, int64> _.GetInt64(0))
                    return result > 0L
                }
                
                let! exists     = itExists "ensured"
                let! alsoExists = itExists "idx_ensured_key"
                Expect.isFalse exists     "The table should not exist already"
                Expect.isFalse alsoExists "The key index should not exist already"
        
                do! Definition.EnsureTable "ensured"
                let! exists'     = itExists "ensured"
                let! alsoExists' = itExists "idx_ensured_key"
                Expect.isTrue exists'    "The table should now exist"
                Expect.isTrue alsoExists' "The key index should now exist"
            }
        ]
        testList "Insert" [
            testTask "succeeds" {
                use! db = Db.buildDb ()
                let! before = Find.All<SubDocument> Db.tableName
                Expect.hasCountOf before 0u isTrue "There should be no documents in the table"
                do! Insert(
                        Db.tableName,
                        JsonDocument(Id = "turkey", Sub = Some (SubDocument(Foo = "gobble", Bar = "gobble"))))
                let! after = Find.All<JsonDocument> Db.tableName
                Expect.hasCountOf after 1u isTrue "There should have been one document inserted"
            }
            testTask "fails for duplicate key" {
                use! db = Db.buildDb ()
                do! Insert(Db.tableName, JsonDocument(Id = "test"))
                Expect.throws
                    (fun () ->
                        Insert(Db.tableName, JsonDocument(Id = "test")) |> Async.AwaitTask |> Async.RunSynchronously)
                    "An exception should have been raised for duplicate document ID insert"
            }
        ]
        testList "Save" [
            testTask "succeeds when a document is inserted" {
                use! db = Db.buildDb ()
                let! before = Find.All<JsonDocument> Db.tableName
                Expect.hasCountOf before 0u isTrue "There should be no documents in the table"
        
                let testDoc = JsonDocument(Id = "test", Sub = Some (SubDocument(Foo = "a", Bar = "b")))
                do! Save(Db.tableName, JsonDocument(Id = "test", Sub = Some (SubDocument(Foo = "a", Bar = "b"))))
                let! after = Find.All<JsonDocument> Db.tableName
                Expect.hasCountOf after 1u isTrue "There should have been one document inserted"
            }
            testTask "succeeds when a document is updated" {
                use! db = Db.buildDb ()
                do! Insert(Db.tableName, JsonDocument(Id = "test", Sub = Some (SubDocument(Foo = "a", Bar = "b"))))
        
                let! before = Find.ById<string, JsonDocument>(Db.tableName, "test")
                if isNull before then Expect.isTrue false "There should have been a document returned"
                let before = before :> JsonDocument
                Expect.equal before.Id "test" "The document is not correct"
                Expect.isSome before.Sub "There should have been a sub-document"
                Expect.equal before.Sub.Value.Foo "a" "The document is not correct"
                Expect.equal before.Sub.Value.Bar "b" "The document is not correct"
        
                do! Save(Db.tableName, JsonDocument(Id = "test"))
                let! after = Find.ById<string, JsonDocument>(Db.tableName, "test")
                if isNull after then Expect.isTrue false "There should have been a document returned post-update"
                let after = after :> JsonDocument
                Expect.equal after.Id "test" "The updated document is not correct"
                Expect.isNone after.Sub "There should not have been a sub-document in the updated document"
            }
        ]
        testList "Count" [
            testTask "All succeeds" {
                use! db = Db.buildDb ()
                do! loadDocs ()
        
                let! theCount = Count.All Db.tableName
                Expect.equal theCount 5L "There should have been 5 matching documents"
            }
            testTask "ByFieldEquals succeeds" {
                use! db = Db.buildDb ()
                do! loadDocs ()
        
                let! theCount = Count.ByFieldEquals(Db.tableName, "Value", "purple")
                Expect.equal theCount 2L "There should have been 2 matching documents"
            }
        ]
        testList "Exists" [
            testList "ById" [
                testTask "succeeds when a document exists" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! exists = Exists.ById(Db.tableName, "three")
                    Expect.isTrue exists "There should have been an existing document"
                }
                testTask "succeeds when a document does not exist" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! exists = Exists.ById(Db.tableName, "seven")
                    Expect.isFalse exists "There should not have been an existing document"
                }
            ]
            testList "ByFieldEquals" [
                testTask "succeeds when documents exist" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! exists = Exists.ByFieldEquals(Db.tableName, "NumValue", 10)
                    Expect.isTrue exists "There should have been existing documents"
                }
                testTask "succeeds when no matching documents exist" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! exists = Exists.ByFieldEquals(Db.tableName, "Nothing", "none")
                    Expect.isFalse exists "There should not have been any existing documents"
                }
            ]
        ]
        testList "Find" [
            testList "All" [
                testTask "succeeds when there is data" {
                    use! db = Db.buildDb ()
        
                    do! Insert(Db.tableName, JsonDocument(Id = "one", Value = "two"))
                    do! Insert(Db.tableName, JsonDocument(Id = "three", Value = "four"))
                    do! Insert(Db.tableName, JsonDocument(Id = "five", Value = "six"))
        
                    let! results = Find.All<SubDocument> Db.tableName
                    Expect.hasCountOf results 3u isTrue "There should have been 3 documents returned"
                }
                testTask "succeeds when there is no data" {
                    use! db = Db.buildDb ()
                    let! results = Find.All<SubDocument> Db.tableName
                    Expect.hasCountOf results 0u isTrue "There should have been no documents returned"
                }
            ]
            testList "ById" [
                testTask "succeeds when a document is found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc = Find.ById<string, JsonDocument>(Db.tableName, "two")
                    if isNull doc then Expect.isTrue false "There should have been a document returned"
                    Expect.equal (doc :> JsonDocument).Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc = Find.ById<string, JsonDocument>(Db.tableName, "three hundred eighty-seven")
                    Expect.isNull doc "There should not have been a document returned"
                }
            ]
            testList "ByFieldEquals" [
                testTask "succeeds when documents are found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! docs = Find.ByFieldEquals<JsonDocument>(Db.tableName, "Sub.Foo", "green")
                    Expect.hasCountOf docs 2u isTrue "There should have been two documents returned"
                }
                testTask "succeeds when documents are not found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! docs = Find.ByFieldEquals<JsonDocument>(Db.tableName, "Value", "mauve")
                    Expect.hasCountOf docs 0u isTrue "There should have been no documents returned"
                }
            ]
            testList "FirstByFieldEquals" [
                testTask "succeeds when a document is found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc = Find.FirstByFieldEquals<JsonDocument>(Db.tableName, "Value", "another")
                    if isNull doc then Expect.isTrue false "There should have been a document returned"
                    Expect.equal (doc :> JsonDocument).Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when multiple documents are found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc = Find.FirstByFieldEquals<JsonDocument>(Db.tableName, "Sub.Foo", "green")
                    if isNull doc then Expect.isTrue false "There should have been a document returned"
                    Expect.contains [ "two"; "four" ] (doc :> JsonDocument).Id "An incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc = Find.FirstByFieldEquals<JsonDocument>(Db.tableName, "Value", "absent")
                    Expect.isNull doc "There should not have been a document returned"
                }
            ]
        ]
        testList "Update" [
            testList "Full" [
                testTask "succeeds when a document is updated" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let testDoc = JsonDocument(Id = "one", Sub = Some (SubDocument(Foo = "blue", Bar = "red")))
                    do! Update.Full(Db.tableName, "one", testDoc)
                    let! after = Find.ById<string, JsonDocument>(Db.tableName, "one")
                    if isNull after then Expect.isTrue false "There should have been a document returned post-update"
                    let after = after :> JsonDocument
                    Expect.equal after.Id "one" "The updated document is not correct"
                    Expect.isSome after.Sub "The updated document should have had a sub-document"
                    Expect.equal after.Sub.Value.Foo "blue" "The updated sub-document is not correct"
                    Expect.equal after.Sub.Value.Bar "red" "The updated sub-document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use! db = Db.buildDb ()
        
                    let! before = Find.All<JsonDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.Full(
                            Db.tableName,
                            "test",
                            JsonDocument(Id = "x", Sub = Some (SubDocument(Foo = "blue", Bar = "red"))))
                }
            ]
            testList "FullFunc" [
                testTask "succeeds when a document is updated" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    do! Update.FullFunc(
                        Db.tableName,
                        System.Func<JsonDocument, string> _.Id,
                        JsonDocument(Id = "one", Value = "le un", NumValue = 1, Sub = None))
                    let! after = Find.ById<string, JsonDocument>(Db.tableName, "one")
                    if isNull after then Expect.isTrue false "There should have been a document returned post-update"
                    let after = after :> JsonDocument
                    Expect.equal after.Id "one" "The updated document is incorrect"
                    Expect.equal after.Value "le un" "The updated document is incorrect"
                    Expect.equal after.NumValue 1 "The updated document is incorrect"
                    Expect.isNone after.Sub "The updated document should not have a sub-document"
                }
                testTask "succeeds when no document is updated" {
                    use! db = Db.buildDb ()
        
                    let! before = Find.All<JsonDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.FullFunc(
                        Db.tableName,
                        System.Func<JsonDocument, string> _.Id,
                        JsonDocument(Id = "one", Value = "le un", NumValue = 1, Sub = None))
                }
            ]
            testList "PartialById" [
                testTask "succeeds when a document is updated" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
                    
                    do! Update.PartialById(Db.tableName, "one", {| NumValue = 44 |})
                    let! after = Find.ById<string, JsonDocument>(Db.tableName, "one")
                    if isNull after then Expect.isTrue false "There should have been a document returned post-update"
                    let after = after :> JsonDocument
                    Expect.equal after.Id "one" "The updated document is not correct"
                    Expect.equal after.NumValue 44 "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use! db = Db.buildDb ()
        
                    let! before = Find.All<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.PartialById(Db.tableName, "test", {| Foo = "green" |})
                }
            ]
            testList "PartialByFieldEquals" [
                testTask "succeeds when a document is updated" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
                    
                    do! Update.PartialByFieldEquals(Db.tableName, "Value", "purple", {| NumValue = 77 |})
                    let! after = Count.ByFieldEquals(Db.tableName, "NumValue", 77)
                    Expect.equal after 2L "There should have been 2 documents returned"
                }
                testTask "succeeds when no document is updated" {
                    use! db = Db.buildDb ()
        
                    let! before = Find.All<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.PartialByFieldEquals(Db.tableName, "Value", "burgundy", {| Foo = "green" |})
                }
            ]
        ]
        testList "Delete" [
            testList "byId" [
                testTask "succeeds when a document is deleted" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    do! Delete.ById(Db.tableName, "four")
                    let! remaining = Count.All Db.tableName
                    Expect.equal remaining 4L "There should have been 4 documents remaining"
                }
                testTask "succeeds when a document is not deleted" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    do! Delete.ById(Db.tableName, "thirty")
                    let! remaining = Count.All Db.tableName
                    Expect.equal remaining 5L "There should have been 5 documents remaining"
                }
            ]
            testList "ByFieldEquals" [
                testTask "succeeds when documents are deleted" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    do! Delete.ByFieldEquals(Db.tableName, "Value", "purple")
                    let! remaining = Count.All Db.tableName
                    Expect.equal remaining 3L "There should have been 3 documents remaining"
                }
                testTask "succeeds when documents are not deleted" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    do! Delete.ByFieldEquals(Db.tableName, "Value", "crimson")
                    let! remaining = Count.All Db.tableName
                    Expect.equal remaining 5L "There should have been 5 documents remaining"
                }
            ]
        ]
        testList "Custom" [
            testList "Single" [
                testTask "succeeds when a row is found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc =
                        Custom.Single(
                            $"SELECT data FROM {Db.tableName} WHERE data ->> 'Id' = @id",
                            [ SqliteParameter("@id", "one") ],
                            FromData<JsonDocument>)
                    if isNull doc then Expect.isTrue false "There should have been a document returned"
                    Expect.equal (doc :> JsonDocument).Id "one" "The incorrect document was returned"
                }
                testTask "succeeds when a row is not found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc =
                        Custom.Single(
                            $"SELECT data FROM {Db.tableName} WHERE data ->> 'Id' = @id",
                            [ SqliteParameter("@id", "eighty") ],
                            FromData<JsonDocument>)
                    Expect.isNull doc "There should not have been a document returned"
                }
            ]
            testList "List" [
                testTask "succeeds when data is found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! docs = Custom.List(Query.SelectFromTable Db.tableName, [], FromData<JsonDocument>)
                    Expect.hasCountOf docs 5u isTrue "There should have been 5 documents returned"
                }
                testTask "succeeds when data is not found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! docs =
                        Custom.List(
                            $"SELECT data FROM {Db.tableName} WHERE data ->> 'NumValue' > @value",
                            [ SqliteParameter("@value", 100) ],
                            FromData<JsonDocument>)
                    Expect.isEmpty docs "There should have been no documents returned"
                }
            ]
            testList "NonQuery" [
                testTask "succeeds when operating on data" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()

                    do! Custom.NonQuery($"DELETE FROM {Db.tableName}", [])

                    let! remaining = Count.All Db.tableName
                    Expect.equal remaining 0L "There should be no documents remaining in the table"
                }
                testTask "succeeds when no data matches where clause" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()

                    do! Custom.NonQuery(
                            $"DELETE FROM {Db.tableName} WHERE data ->> 'NumValue' > @value",
                            [ SqliteParameter("@value", 100) ])

                    let! remaining = Count.All Db.tableName
                    Expect.equal remaining 5L "There should be 5 documents remaining in the table"
                }
            ]
            testTask "scalar succeeds" {
                use! db = Db.buildDb ()
        
                let! nbr = Custom.Scalar("SELECT 5 AS test_value", [], System.Func<SqliteDataReader, int> _.GetInt32(0))
                Expect.equal nbr 5 "The query should have returned the number 5"
            }
        ]
        test "clean up database" {
            Configuration.UseConnectionString "data source=:memory:"
        }
    ]
    |> testSequenced

let all = testList "Documents" [ unitTests; integrationTests ]
