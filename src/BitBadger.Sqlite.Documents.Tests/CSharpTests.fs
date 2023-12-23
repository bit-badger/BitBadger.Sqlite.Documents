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

module FS = BitBadger.Sqlite.FSharp.Documents

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
        testList "Op.convert" [
            test "succeeds for EQ" {
                Expect.equal (Op.convert Op.EQ) FS.Op.EQ "The equals operator was not correct"
            }
            test "succeeds for GT" {
                Expect.equal (Op.convert Op.GT) FS.Op.GT "The greater than operator was not correct"
            }
            test "succeeds for GE" {
                Expect.equal (Op.convert Op.GE) FS.Op.GE "The greater than or equal to operator was not correct"
            }
            test "succeeds for LT" {
                Expect.equal (Op.convert Op.LT) FS.Op.LT "The less than operator was not correct"
            }
            test "succeeds for LE" {
                Expect.equal (Op.convert Op.LE) FS.Op.LE "The less than or equal to operator was not correct"
            }
            test "succeeds for NE" {
                Expect.equal (Op.convert Op.NE) FS.Op.NE "The not equal to operator was not correct"
            }
            test "succeeds for EX" {
                Expect.equal (Op.convert Op.EX) FS.Op.EX """The "exists" operator ws not correct"""
            }
            test "succeeds for NEX" {
                Expect.equal (Op.convert Op.NEX) FS.Op.NEX """The "not exists" operator ws not correct"""
            }
            test "fails for invalid operator" {
                Expect.throws
                    (fun () -> Op.convert (box 8 :?> Op) |> ignore)
                    "Conversion to an invalid value should have thrown an exception"
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
            testList "WhereByField" [
                test "succeeds when a logical operator is passed" {
                    Expect.equal
                        (Query.WhereByField("theField", Op.EQ, "@test"))
                        "data ->> 'theField' = @test"
                        "WHERE clause not correct"
                }
                test "succeeds when an existence operator is passed" {
                    Expect.equal
                        (Query.WhereByField("something", Op.EX, ""))
                        "data ->> 'something' IS NOT NULL"
                        "WHERE clause not correct"
                }
            ]
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
                test "ByField succeeds" {
                    Expect.equal
                        (Query.Count.ByField(Db.tableName, "thatField", Op.GT))
                        $"SELECT COUNT(*) AS it FROM {Db.tableName} WHERE data ->> 'thatField' > @field"
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
                test "ByField succeeds" {
                    Expect.equal
                        (Query.Exists.ByField(Db.tableName, "Test", Op.LE))
                        $"SELECT EXISTS (SELECT 1 FROM {Db.tableName} WHERE data ->> 'Test' <= @field) AS it"
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
                test "ByField succeeds" {
                    Expect.equal
                        (Query.Find.ByField(Db.tableName, "Golf", Op.NE))
                        $"SELECT data FROM {Db.tableName} WHERE data ->> 'Golf' <> @field"
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
                test "PartialByField succeeds" {
                    Expect.equal
                        (Query.Update.PartialByField(Db.tableName, "Part", Op.EQ))
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
                test "ByField succeeds" {
                    Expect.equal
                        (Query.Delete.ByField(Db.tableName, "gone", Op.GT))
                        $"DELETE FROM {Db.tableName} WHERE data ->> 'gone' > @field"
                        "DELETE by JSON containment query not correct"
                }
            ]
        ]
    ]

let isTrue<'T> (_ : 'T) = true

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
            testTask "ByField succeeds" {
                use! db = Db.buildDb ()
                do! loadDocs ()
        
                let! theCount = Count.ByField(Db.tableName, "Value", Op.EQ, "purple")
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
            testList "ByField" [
                testTask "succeeds when documents exist" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! exists = Exists.ByField(Db.tableName, "NumValue", Op.GE, 10)
                    Expect.isTrue exists "There should have been existing documents"
                }
                testTask "succeeds when no matching documents exist" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! exists = Exists.ByField(Db.tableName, "Nothing", Op.EQ, "none")
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
            testList "ByField" [
                testTask "succeeds when documents are found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! docs = Find.ByField<JsonDocument>(Db.tableName, "NumValue", Op.GT, 15)
                    Expect.hasCountOf docs 2u isTrue "There should have been two documents returned"
                }
                testTask "succeeds when documents are not found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! docs = Find.ByField<JsonDocument>(Db.tableName, "Value", Op.EQ, "mauve")
                    Expect.hasCountOf docs 0u isTrue "There should have been no documents returned"
                }
            ]
            testList "FirstByField" [
                testTask "succeeds when a document is found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc = Find.FirstByField<JsonDocument>(Db.tableName, "Value", Op.EQ, "another")
                    if isNull doc then Expect.isTrue false "There should have been a document returned"
                    Expect.equal (doc :> JsonDocument).Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when multiple documents are found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc = Find.FirstByField<JsonDocument>(Db.tableName, "Sub.Foo", Op.EQ, "green")
                    if isNull doc then Expect.isTrue false "There should have been a document returned"
                    Expect.contains [ "two"; "four" ] (doc :> JsonDocument).Id "An incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc = Find.FirstByField<JsonDocument>(Db.tableName, "Value", Op.EQ, "absent")
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
            testList "PartialByField" [
                testTask "succeeds when a document is updated" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
                    
                    do! Update.PartialByField(Db.tableName, "Value", Op.EQ, "purple", {| NumValue = 77 |})
                    let! after = Count.ByField(Db.tableName, "NumValue", Op.EQ, 77)
                    Expect.equal after 2L "There should have been 2 documents returned"
                }
                testTask "succeeds when no document is updated" {
                    use! db = Db.buildDb ()
        
                    let! before = Find.All<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.PartialByField(Db.tableName, "Value", Op.EQ, "burgundy", {| Foo = "green" |})
                }
            ]
        ]
        testList "Delete" [
            testList "ById" [
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
            testList "ByField" [
                testTask "succeeds when documents are deleted" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    do! Delete.ByField(Db.tableName, "Value", Op.NE, "purple")
                    let! remaining = Count.All Db.tableName
                    Expect.equal remaining 2L "There should have been 2 documents remaining"
                }
                testTask "succeeds when documents are not deleted" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    do! Delete.ByField(Db.tableName, "Value", Op.EQ, "crimson")
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
            testTask "Scalar succeeds" {
                use! db = Db.buildDb ()
        
                let! nbr = Custom.Scalar("SELECT 5 AS test_value", [], System.Func<SqliteDataReader, int> _.GetInt32(0))
                Expect.equal nbr 5 "The query should have returned the number 5"
            }
        ]
        testList "Extensions" [
            testTask "EnsureTable succeeds" {
                use! db   = Db.buildDb ()
                use  conn = Configuration.DbConn()
                let itExists (name: string) = task {
                    let! result =
                        conn.CustomScalar(
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
            testList "Insert" [
                testTask "succeeds" {
                    use! db     = Db.buildDb ()
                    use  conn   = Configuration.DbConn()
                    let! before = conn.FindAll<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should be no documents in the table"
                    do! conn.Insert(
                            Db.tableName,
                            JsonDocument(Id = "turkey", Sub = Some (SubDocument(Foo = "gobble", Bar = "gobble"))))
                    let! after = conn.FindAll<JsonDocument> Db.tableName
                    Expect.hasCountOf after 1u isTrue "There should have been one document inserted"
                }
                testTask "fails for duplicate key" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! conn.Insert(Db.tableName, JsonDocument(Id = "test"))
                    Expect.throws
                        (fun () ->
                            conn.Insert(Db.tableName, JsonDocument(Id = "test"))
                            |> Async.AwaitTask
                            |> Async.RunSynchronously)
                        "An exception should have been raised for duplicate document ID insert"
                }
            ]
            testList "Save" [
                testTask "succeeds when a document is inserted" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    let! before = conn.FindAll<JsonDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should be no documents in the table"
            
                    do! conn.Save(
                            Db.tableName,
                            JsonDocument(Id = "test", Sub = Some (SubDocument(Foo = "a", Bar = "b"))))
                    let! after = conn.FindAll<JsonDocument> Db.tableName
                    Expect.hasCountOf after 1u isTrue "There should have been one document inserted"
                }
                testTask "succeeds when a document is updated" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! conn.Insert(
                            Db.tableName,
                            JsonDocument(Id = "test", Sub = Some (SubDocument(Foo = "a", Bar = "b"))))
            
                    let! before = conn.FindById<string, JsonDocument>(Db.tableName, "test")
                    if isNull before then Expect.isTrue false "There should have been a document returned"
                    let before = before :> JsonDocument
                    Expect.equal before.Id "test" "The document is not correct"
                    Expect.isSome before.Sub "There should have been a sub-document"
                    Expect.equal before.Sub.Value.Foo "a" "The document is not correct"
                    Expect.equal before.Sub.Value.Bar "b" "The document is not correct"
            
                    do! Save(Db.tableName, JsonDocument(Id = "test"))
                    let! after = conn.FindById<string, JsonDocument>(Db.tableName, "test")
                    if isNull after then Expect.isTrue false "There should have been a document returned post-update"
                    let after = after :> JsonDocument
                    Expect.equal after.Id "test" "The updated document is not correct"
                    Expect.isNone after.Sub "There should not have been a sub-document in the updated document"
                }
            ]
            testTask "CountAll succeeds" {
                use! db   = Db.buildDb ()
                use  conn = Configuration.DbConn()
                do! loadDocs ()
        
                let! theCount = conn.CountAll Db.tableName
                Expect.equal theCount 5L "There should have been 5 matching documents"
            }
            testTask "CountByField succeeds" {
                use! db   = Db.buildDb ()
                use  conn = Configuration.DbConn()
                do! loadDocs ()
        
                let! theCount = conn.CountByField(Db.tableName, "Value", Op.EQ, "purple")
                Expect.equal theCount 2L "There should have been 2 matching documents"
            }
            testList "ExistsById" [
                testTask "succeeds when a document exists" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! exists = conn.ExistsById(Db.tableName, "three")
                    Expect.isTrue exists "There should have been an existing document"
                }
                testTask "succeeds when a document does not exist" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! exists = conn.ExistsById(Db.tableName, "seven")
                    Expect.isFalse exists "There should not have been an existing document"
                }
            ]
            testList "ExistsByField" [
                testTask "succeeds when documents exist" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! exists = conn.ExistsByField(Db.tableName, "NumValue", Op.GE, 10)
                    Expect.isTrue exists "There should have been existing documents"
                }
                testTask "succeeds when no matching documents exist" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! exists = conn.ExistsByField(Db.tableName, "Nothing", Op.EQ, "none")
                    Expect.isFalse exists "There should not have been any existing documents"
                }
            ]
            testList "FindAll" [
                testTask "succeeds when there is data" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
        
                    do! conn.Insert(Db.tableName, JsonDocument(Id = "one", Value = "two"))
                    do! conn.Insert(Db.tableName, JsonDocument(Id = "three", Value = "four"))
                    do! conn.Insert(Db.tableName, JsonDocument(Id = "five", Value = "six"))
        
                    let! results = conn.FindAll<SubDocument> Db.tableName
                    Expect.hasCountOf results 3u isTrue "There should have been 3 documents returned"
                }
                testTask "succeeds when there is no data" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    let! results = conn.FindAll<SubDocument> Db.tableName
                    Expect.hasCountOf results 0u isTrue "There should have been no documents returned"
                }
            ]
            testList "FindById" [
                testTask "succeeds when a document is found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! doc = conn.FindById<string, JsonDocument>(Db.tableName, "two")
                    if isNull doc then Expect.isTrue false "There should have been a document returned"
                    Expect.equal (doc :> JsonDocument).Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! doc = conn.FindById<string, JsonDocument>(Db.tableName, "three hundred eighty-seven")
                    Expect.isNull doc "There should not have been a document returned"
                }
            ]
            testList "FindByField" [
                testTask "succeeds when documents are found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! docs = conn.FindByField<JsonDocument>(Db.tableName, "NumValue", Op.GT, 15)
                    Expect.hasCountOf docs 2u isTrue "There should have been two documents returned"
                }
                testTask "succeeds when documents are not found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! docs = conn.FindByField<JsonDocument>(Db.tableName, "Value", Op.EQ, "mauve")
                    Expect.hasCountOf docs 0u isTrue "There should have been no documents returned"
                }
            ]
            testList "FindFirstByField" [
                testTask "succeeds when a document is found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! doc = conn.FindFirstByField<JsonDocument>(Db.tableName, "Value", Op.EQ, "another")
                    if isNull doc then Expect.isTrue false "There should have been a document returned"
                    Expect.equal (doc :> JsonDocument).Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when multiple documents are found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! doc = conn.FindFirstByField<JsonDocument>(Db.tableName, "Sub.Foo", Op.EQ, "green")
                    if isNull doc then Expect.isTrue false "There should have been a document returned"
                    Expect.contains [ "two"; "four" ] (doc :> JsonDocument).Id "An incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! doc = conn.FindFirstByField<JsonDocument>(Db.tableName, "Value", Op.EQ, "absent")
                    Expect.isNull doc "There should not have been a document returned"
                }
            ]
            testList "UpdateFull" [
                testTask "succeeds when a document is updated" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
                    
                    let testDoc = JsonDocument(Id = "one", Sub = Some (SubDocument(Foo = "blue", Bar = "red")))
                    do! conn.UpdateFull(Db.tableName, "one", testDoc)
                    let! after = conn.FindById<string, JsonDocument>(Db.tableName, "one")
                    if isNull after then Expect.isTrue false "There should have been a document returned post-update"
                    let after = after :> JsonDocument
                    Expect.equal after.Id "one" "The updated document is not correct"
                    Expect.isSome after.Sub "The updated document should have had a sub-document"
                    Expect.equal after.Sub.Value.Foo "blue" "The updated sub-document is not correct"
                    Expect.equal after.Sub.Value.Bar "red" "The updated sub-document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use! db     = Db.buildDb ()
                    use  conn   = Configuration.DbConn()
                    let! before = conn.FindAll<JsonDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! conn.UpdateFull(
                            Db.tableName,
                            "test",
                            JsonDocument(Id = "x", Sub = Some (SubDocument(Foo = "blue", Bar = "red"))))
                }
            ]
            testList "UpdateFullFunc" [
                testTask "succeeds when a document is updated" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    do! conn.UpdateFullFunc(
                        Db.tableName,
                        System.Func<JsonDocument, string> _.Id,
                        JsonDocument(Id = "one", Value = "le un", NumValue = 1, Sub = None))
                    let! after = conn.FindById<string, JsonDocument>(Db.tableName, "one")
                    if isNull after then Expect.isTrue false "There should have been a document returned post-update"
                    let after = after :> JsonDocument
                    Expect.equal after.Id "one" "The updated document is incorrect"
                    Expect.equal after.Value "le un" "The updated document is incorrect"
                    Expect.equal after.NumValue 1 "The updated document is incorrect"
                    Expect.isNone after.Sub "The updated document should not have a sub-document"
                }
                testTask "succeeds when no document is updated" {
                    use! db     = Db.buildDb ()
                    use  conn   = Configuration.DbConn()
                    let! before = conn.FindAll<JsonDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! conn.UpdateFullFunc(
                        Db.tableName,
                        System.Func<JsonDocument, string> _.Id,
                        JsonDocument(Id = "one", Value = "le un", NumValue = 1, Sub = None))
                }
            ]
            testList "UpdatePartialById" [
                testTask "succeeds when a document is updated" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
                    
                    do! conn.UpdatePartialById(Db.tableName, "one", {| NumValue = 44 |})
                    let! after = conn.FindById<string, JsonDocument>(Db.tableName, "one")
                    if isNull after then Expect.isTrue false "There should have been a document returned post-update"
                    let after = after :> JsonDocument
                    Expect.equal after.Id "one" "The updated document is not correct"
                    Expect.equal after.NumValue 44 "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use! db     = Db.buildDb ()
                    use  conn   = Configuration.DbConn()
                    let! before = conn.FindAll<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! conn.UpdatePartialById(Db.tableName, "test", {| Foo = "green" |})
                }
            ]
            testList "UpdatePartialByField" [
                testTask "succeeds when a document is updated" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
                    
                    do! conn.UpdatePartialByField(Db.tableName, "Value", Op.EQ, "purple", {| NumValue = 77 |})
                    let! after = conn.CountByField(Db.tableName, "NumValue", Op.EQ, 77)
                    Expect.equal after 2L "There should have been 2 documents returned"
                }
                testTask "succeeds when no document is updated" {
                    use! db     = Db.buildDb ()
                    use  conn   = Configuration.DbConn()
                    let! before = conn.FindAll<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! conn.UpdatePartialByField(Db.tableName, "Value", Op.EQ, "burgundy", {| Foo = "green" |})
                }
            ]
            testList "DeleteById" [
                testTask "succeeds when a document is deleted" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    do! conn.DeleteById(Db.tableName, "four")
                    let! remaining = conn.CountAll Db.tableName
                    Expect.equal remaining 4L "There should have been 4 documents remaining"
                }
                testTask "succeeds when a document is not deleted" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    do! conn.DeleteById(Db.tableName, "thirty")
                    let! remaining = conn.CountAll Db.tableName
                    Expect.equal remaining 5L "There should have been 5 documents remaining"
                }
            ]
            testList "DeleteByField" [
                testTask "succeeds when documents are deleted" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    do! conn.DeleteByField(Db.tableName, "Value", Op.NE, "purple")
                    let! remaining = conn.CountAll Db.tableName
                    Expect.equal remaining 2L "There should have been 2 documents remaining"
                }
                testTask "succeeds when documents are not deleted" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    do! conn.DeleteByField(Db.tableName, "Value", Op.EQ, "crimson")
                    let! remaining = Count.All Db.tableName
                    Expect.equal remaining 5L "There should have been 5 documents remaining"
                }
            ]
            testList "CustomSingle" [
                testTask "succeeds when a row is found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! doc =
                        conn.CustomSingle(
                            $"SELECT data FROM {Db.tableName} WHERE data ->> 'Id' = @id",
                            [ SqliteParameter("@id", "one") ],
                            FromData<JsonDocument>)
                    if isNull doc then Expect.isTrue false "There should have been a document returned"
                    Expect.equal (doc :> JsonDocument).Id "one" "The incorrect document was returned"
                }
                testTask "succeeds when a row is not found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! doc =
                        conn.CustomSingle(
                            $"SELECT data FROM {Db.tableName} WHERE data ->> 'Id' = @id",
                            [ SqliteParameter("@id", "eighty") ],
                            FromData<JsonDocument>)
                    Expect.isNull doc "There should not have been a document returned"
                }
            ]
            testList "CustomList" [
                testTask "succeeds when data is found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! docs = conn.CustomList(Query.SelectFromTable Db.tableName, [], FromData<JsonDocument>)
                    Expect.hasCountOf docs 5u isTrue "There should have been 5 documents returned"
                }
                testTask "succeeds when data is not found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()
        
                    let! docs =
                        conn.CustomList(
                            $"SELECT data FROM {Db.tableName} WHERE data ->> 'NumValue' > @value",
                            [ SqliteParameter("@value", 100) ],
                            FromData<JsonDocument>)
                    Expect.isEmpty docs "There should have been no documents returned"
                }
            ]
            testList "CustomNonQuery" [
                testTask "succeeds when operating on data" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()

                    do! conn.CustomNonQuery($"DELETE FROM {Db.tableName}", [])

                    let! remaining = conn.CountAll Db.tableName
                    Expect.equal remaining 0L "There should be no documents remaining in the table"
                }
                testTask "succeeds when no data matches where clause" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.DbConn()
                    do! loadDocs ()

                    do! conn.CustomNonQuery(
                            $"DELETE FROM {Db.tableName} WHERE data ->> 'NumValue' > @value",
                            [ SqliteParameter("@value", 100) ])

                    let! remaining = conn.CountAll Db.tableName
                    Expect.equal remaining 5L "There should be 5 documents remaining in the table"
                }
            ]
            testTask "CustomScalar succeeds" {
                use! db   = Db.buildDb ()
                use  conn = Configuration.DbConn()
        
                let! nbr =
                    conn.CustomScalar("SELECT 5 AS test_value", [], System.Func<SqliteDataReader, int> _.GetInt32(0))
                Expect.equal nbr 5 "The query should have returned the number 5"
            }
        ]
        test "clean up database" {
            Configuration.UseConnectionString "data source=:memory:"
        }
    ]
    |> testSequenced

let all = testList "Documents" [ unitTests; integrationTests ]
