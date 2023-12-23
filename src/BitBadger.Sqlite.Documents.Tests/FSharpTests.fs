module FSharpTests

open BitBadger.Sqlite.Documents
open BitBadger.Sqlite.FSharp.Documents
open Expecto
open Microsoft.Data.Sqlite

type SubDocument =
    { Foo: string
      Bar: string }

type JsonDocument =
    { Id: string
      Value: string
      NumValue: int
      Sub: SubDocument option }

let emptyDoc = { Id = ""; Value = ""; NumValue = 0; Sub = None }

/// Tests which do not hit the database
let unitTests =
    testList "Unit" [
        testList "Definition" [
            test "createTable succeeds" {
                Expect.equal (Definition.createTable Db.tableName)
                    $"CREATE TABLE IF NOT EXISTS {Db.tableName} (data TEXT NOT NULL)"
                    "CREATE TABLE statement not constructed correctly"
            }
            test "createKey succeeds" {
                Expect.equal (Definition.createKey Db.tableName)
                    $"CREATE UNIQUE INDEX IF NOT EXISTS idx_{Db.tableName}_key ON {Db.tableName} ((data ->> 'Id'))"
                    "CREATE INDEX for key statement not constructed correctly"
            }
        ]
        testList "Op" [
            test "EQ succeeds" {
                Expect.equal (string EQ) "=" "The equals operator was not correct"
            }
            test "GT succeeds" {
                Expect.equal (string GT) ">" "The greater than operator was not correct"
            }
            test "GE succeeds" {
                Expect.equal (string GE) ">=" "The greater than or equal to operator was not correct"
            }
            test "LT succeeds" {
                Expect.equal (string LT) "<" "The less than operator was not correct"
            }
            test "LE succeeds" {
                Expect.equal (string LE) "<=" "The less than or equal to operator was not correct"
            }
            test "NE succeeds" {
                Expect.equal (string NE) "<>" "The not equal to operator was not correct"
            }
            test "EX succeeds" {
                Expect.equal (string EX) "IS NOT NULL" """The "exists" operator ws not correct"""
            }
            test "NEX succeeds" {
                Expect.equal (string NEX) "IS NULL" """The "not exists" operator ws not correct"""
            }
        ]
        testList "Query" [
            test "selectFromTable succeeds" {
                Expect.equal
                    (Query.selectFromTable Db.tableName)
                    $"SELECT data FROM {Db.tableName}"
                    "SELECT statement not correct"
            }
            test "whereById succeeds" {
                Expect.equal (Query.whereById "@id") "data ->> 'Id' = @id" "WHERE clause not correct"
            }
            testList "whereByField" [
                test "succeeds when a logical operator is passed" {
                    Expect.equal
                        (Query.whereByField "theField" GT "@test")
                        "data ->> 'theField' > @test"
                        "WHERE clause not correct"
                }
                test "succeeds when an existence operator is passed" {
                    Expect.equal
                        (Query.whereByField "thatField" NEX "")
                        "data ->> 'thatField' IS NULL"
                        "WHERE clause not correct"
                }
            ]
            test "insert succeeds" {
                Expect.equal
                    (Query.insert Db.tableName)
                    $"INSERT INTO {Db.tableName} VALUES (@data)"
                    "INSERT statement not correct"
            }
            test "save succeeds" {
                Expect.equal
                    (Query.save Db.tableName)
                    $"INSERT INTO {Db.tableName} VALUES (@data) ON CONFLICT ((data ->> 'Id')) DO UPDATE SET data = EXCLUDED.data"
                    "INSERT ON CONFLICT UPDATE statement not correct"
            }
            testList "Count" [
                test "all succeeds" {
                    Expect.equal
                        (Query.Count.all Db.tableName)
                        $"SELECT COUNT(*) AS it FROM {Db.tableName}"
                        "Count query not correct"
                }
                test "byField succeeds" {
                    Expect.equal
                        (Query.Count.byField Db.tableName "thatField" EQ)
                        $"SELECT COUNT(*) AS it FROM {Db.tableName} WHERE data ->> 'thatField' = @field"
                        "JSON field text comparison count query not correct"
                }
            ]
            testList "Exists" [
                test "byId succeeds" {
                    Expect.equal
                        (Query.Exists.byId Db.tableName)
                        $"SELECT EXISTS (SELECT 1 FROM {Db.tableName} WHERE data ->> 'Id' = @id) AS it"
                        "ID existence query not correct"
                }
                test "byField succeeds" {
                    Expect.equal
                        (Query.Exists.byField Db.tableName "Test" LT)
                        $"SELECT EXISTS (SELECT 1 FROM {Db.tableName} WHERE data ->> 'Test' < @field) AS it"
                        "JSON field text comparison exists query not correct"
                }
            ]
            testList "Find" [
                test "byId succeeds" {
                    Expect.equal
                        (Query.Find.byId Db.tableName)
                        $"SELECT data FROM {Db.tableName} WHERE data ->> 'Id' = @id"
                        "SELECT by ID query not correct"
                }
                test "byField succeeds" {
                    Expect.equal
                        (Query.Find.byField Db.tableName "Golf" GE)
                        $"SELECT data FROM {Db.tableName} WHERE data ->> 'Golf' >= @field"
                        "SELECT by JSON text comparison query not correct"
                }
            ]
            testList "Update" [
                test "full succeeds" {
                    Expect.equal
                        (Query.Update.full Db.tableName)
                        $"UPDATE {Db.tableName} SET data = @data WHERE data ->> 'Id' = @id"
                        "UPDATE full statement not correct"
                }
                test "partialById succeeds" {
                    Expect.equal
                        (Query.Update.partialById Db.tableName)
                        $"UPDATE {Db.tableName} SET data = json_patch(data, json(@data)) WHERE data ->> 'Id' = @id"
                        "UPDATE partial by ID statement not correct"
                }
                test "partialByField succeeds" {
                    Expect.equal
                        (Query.Update.partialByField Db.tableName "Part" NE)
                        $"UPDATE {Db.tableName} SET data = json_patch(data, json(@data)) WHERE data ->> 'Part' <> @field"
                        "UPDATE partial by JSON containment statement not correct"
                }
            ]
            testList "Delete" [
                test "byId succeeds" {
                    Expect.equal
                        (Query.Delete.byId Db.tableName)
                        $"DELETE FROM {Db.tableName} WHERE data ->> 'Id' = @id"
                        "DELETE by ID query not correct"
                }
                test "byField succeeds" {
                    Expect.equal
                        (Query.Delete.byField Db.tableName "gone" EQ)
                        $"DELETE FROM {Db.tableName} WHERE data ->> 'gone' = @field"
                        "DELETE by JSON containment query not correct"
                }
            ]
        ]
    ]

let isTrue<'T> (_ : 'T) = true

let integrationTests =
    let documents = [
        { Id = "one"; Value = "FIRST!"; NumValue = 0; Sub = None }
        { Id = "two"; Value = "another"; NumValue = 10; Sub = Some { Foo = "green"; Bar = "blue" } }
        { Id = "three"; Value = ""; NumValue = 4; Sub = None }
        { Id = "four"; Value = "purple"; NumValue = 17; Sub = Some { Foo = "green"; Bar = "red" } }
        { Id = "five"; Value = "purple"; NumValue = 18; Sub = None }
    ]
    let loadDocs () = backgroundTask {
        for doc in documents do do! insert Db.tableName doc
    }
    testList "Integration" [
        testList "Configuration" [
            test "useConnectionString / connectionString succeed" {
                try
                    Configuration.useConnectionString "Data Source=test.db"
                    Expect.equal
                        Configuration.connectionString
                        (Some "Data Source=test.db;Foreign Keys=True")
                        "Connection string incorrect"
                finally
                    Configuration.useConnectionString "Data Source=:memory:"
            }
            test "useSerializer succeeds" {
                try
                    Configuration.useSerializer
                        { new IDocumentSerializer with
                            member _.Serialize<'T>(it: 'T) : string = """{"Overridden":true}"""
                            member _.Deserialize<'T>(it: string) : 'T = Unchecked.defaultof<'T>
                        }
                    
                    let serialized = Configuration.serializer().Serialize { Foo = "howdy"; Bar = "bye"}
                    Expect.equal serialized """{"Overridden":true}""" "Specified serializer was not used"
                    
                    let deserialized = Configuration.serializer().Deserialize<obj> """{"Something":"here"}"""
                    Expect.isNull deserialized "Specified serializer should have returned null"
                finally
                    Configuration.useSerializer Configuration.defaultSerializer
            }
            test "serializer returns configured serializer" {
                Expect.isTrue (obj.ReferenceEquals(Configuration.defaultSerializer, Configuration.serializer ()))
                    "Serializer should have been the same"
            }
            test "useIdField / idField succeeds" {
                Expect.equal (Configuration.idField ()) "Id" "The default configured ID field was incorrect"
                Configuration.useIdField "id"
                Expect.equal (Configuration.idField ()) "id" "useIdField did not set the ID field"
                Configuration.useIdField "Id"
            }
        ]
        testList "Definition" [
            testTask "ensureTable succeeds" {
                use! db = Db.buildDb ()
                let itExists (name: string) = task {
                    let! result =
                        Custom.scalar
                            $"SELECT EXISTS (SELECT 1 FROM {Db.catalog} WHERE name = @name) AS it"
                            [ SqliteParameter("@name", name) ]
                            _.GetInt64(0)
                    return result > 0
                }
                
                let! exists     = itExists "ensured"
                let! alsoExists = itExists "idx_ensured_key"
                Expect.isFalse exists     "The table should not exist already"
                Expect.isFalse alsoExists "The key index should not exist already"
        
                do! Definition.ensureTable "ensured"
                let! exists'     = itExists "ensured"
                let! alsoExists' = itExists "idx_ensured_key"
                Expect.isTrue exists'    "The table should now exist"
                Expect.isTrue alsoExists' "The key index should now exist"
            }
        ]
        testList "insert" [
            testTask "succeeds" {
                use! db = Db.buildDb ()
                let! before = Find.all<SubDocument> Db.tableName
                Expect.equal before [] "There should be no documents in the table"
        
                let testDoc = { emptyDoc with Id = "turkey"; Sub = Some { Foo = "gobble"; Bar = "gobble" } }
                do! insert Db.tableName testDoc
                let! after = Find.all<JsonDocument> Db.tableName
                Expect.equal after [ testDoc ] "There should have been one document inserted"
            }
            testTask "fails for duplicate key" {
                use! db = Db.buildDb ()
                do! insert Db.tableName { emptyDoc with Id = "test" }
                Expect.throws
                    (fun () ->
                        insert Db.tableName {emptyDoc with Id = "test" } |> Async.AwaitTask |> Async.RunSynchronously)
                    "An exception should have been raised for duplicate document ID insert"
            }
        ]
        testList "save" [
            testTask "succeeds when a document is inserted" {
                use! db = Db.buildDb ()
                let! before = Find.all<JsonDocument> Db.tableName
                Expect.equal before [] "There should be no documents in the table"
        
                let testDoc = { emptyDoc with Id = "test"; Sub = Some { Foo = "a"; Bar = "b" } }
                do! save Db.tableName testDoc
                let! after = Find.all<JsonDocument> Db.tableName
                Expect.equal after [ testDoc ] "There should have been one document inserted"
            }
            testTask "succeeds when a document is updated" {
                use! db = Db.buildDb ()
                let testDoc = { emptyDoc with Id = "test"; Sub = Some { Foo = "a"; Bar = "b" } }
                do! insert Db.tableName testDoc
        
                let! before = Find.byId<string, JsonDocument> Db.tableName "test"
                if Option.isNone before then Expect.isTrue false "There should have been a document returned"
                Expect.equal before.Value testDoc "The document is not correct"
        
                let upd8Doc = { testDoc with Sub = Some { Foo = "c"; Bar = "d" } }
                do! save Db.tableName upd8Doc
                let! after = Find.byId<string, JsonDocument> Db.tableName "test"
                if Option.isNone after then Expect.isTrue false "There should have been a document returned post-update"
                Expect.equal after.Value upd8Doc "The updated document is not correct"
            }
        ]
        testList "Count" [
            testTask "all succeeds" {
                use! db = Db.buildDb ()
                do! loadDocs ()
        
                let! theCount = Count.all Db.tableName
                Expect.equal theCount 5L "There should have been 5 matching documents"
            }
            testTask "byField succeeds" {
                use! db = Db.buildDb ()
                do! loadDocs ()
        
                let! theCount = Count.byField Db.tableName "Value" EQ "purple"
                Expect.equal theCount 2L "There should have been 2 matching documents"
            }
        ]
        testList "Exists" [
            testList "byId" [
                testTask "succeeds when a document exists" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! exists = Exists.byId Db.tableName "three"
                    Expect.isTrue exists "There should have been an existing document"
                }
                testTask "succeeds when a document does not exist" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! exists = Exists.byId Db.tableName "seven"
                    Expect.isFalse exists "There should not have been an existing document"
                }
            ]
            testList "byField" [
                testTask "succeeds when documents exist" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! exists = Exists.byField Db.tableName "NumValue" EQ 10
                    Expect.isTrue exists "There should have been existing documents"
                }
                testTask "succeeds when no matching documents exist" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! exists = Exists.byField Db.tableName "Nothing" LT "none"
                    Expect.isFalse exists "There should not have been any existing documents"
                }
            ]
        ]
        testList "Find" [
            testList "all" [
                testTask "succeeds when there is data" {
                    use! db = Db.buildDb ()
        
                    do! insert Db.tableName { Foo = "one"; Bar = "two" }
                    do! insert Db.tableName { Foo = "three"; Bar = "four" }
                    do! insert Db.tableName { Foo = "five"; Bar = "six" }
        
                    let! results = Find.all<SubDocument> Db.tableName
                    let expected = [
                        { Foo = "one"; Bar = "two" }
                        { Foo = "three"; Bar = "four" }
                        { Foo = "five"; Bar = "six" }
                    ]
                    Expect.equal results expected "There should have been 3 documents returned"
                }
                testTask "succeeds when there is no data" {
                    use! db = Db.buildDb ()
                    let! results = Find.all<SubDocument> Db.tableName
                    Expect.equal results [] "There should have been no documents returned"
                }
            ]
            testList "byId" [
                testTask "succeeds when a document is found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc = Find.byId<string, JsonDocument> Db.tableName "two"
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.equal doc.Value.Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc = Find.byId<string, JsonDocument> Db.tableName "three hundred eighty-seven"
                    Expect.isFalse (Option.isSome doc) "There should not have been a document returned"
                }
            ]
            testList "byField" [
                testTask "succeeds when documents are found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! docs = Find.byField<JsonDocument> Db.tableName "NumValue" GT 15
                    Expect.equal (List.length docs) 2 "There should have been two documents returned"
                }
                testTask "succeeds when documents are not found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! docs = Find.byField<JsonDocument> Db.tableName "NumValue" GT 100
                    Expect.isTrue (List.isEmpty docs) "There should have been no documents returned"
                }
            ]
            testList "firstByField" [
                testTask "succeeds when a document is found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc = Find.firstByField<JsonDocument> Db.tableName "Value" EQ "another"
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.equal doc.Value.Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when multiple documents are found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc = Find.firstByField<JsonDocument> Db.tableName "Sub.Foo" EQ "green"
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.contains [ "two"; "four" ] doc.Value.Id "An incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc = Find.firstByField<JsonDocument> Db.tableName "Value" EQ "absent"
                    Expect.isFalse (Option.isSome doc) "There should not have been a document returned"
                }
            ]
        ]
        testList "Update" [
            testList "full" [
                testTask "succeeds when a document is updated" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let testDoc = { emptyDoc with Id = "one"; Sub = Some { Foo = "blue"; Bar = "red" } }
                    do! Update.full Db.tableName "one" testDoc
                    let! after = Find.byId<string, JsonDocument> Db.tableName "one"
                    if Option.isNone after then
                        Expect.isTrue false "There should have been a document returned post-update"
                    Expect.equal after.Value testDoc "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use! db = Db.buildDb ()
        
                    let! before = Find.all<JsonDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.full
                            Db.tableName
                            "test"
                            { emptyDoc with Id = "x"; Sub = Some { Foo = "blue"; Bar = "red" } }
                }
            ]
            testList "fullFunc" [
                testTask "succeeds when a document is updated" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    do! Update.fullFunc Db.tableName (_.Id) { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                    let! after = Find.byId<string, JsonDocument> Db.tableName "one"
                    if Option.isNone after then
                        Expect.isTrue false "There should have been a document returned post-update"
                    Expect.equal
                        after.Value
                        { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                        "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use! db = Db.buildDb ()
        
                    let! before = Find.all<JsonDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.fullFunc Db.tableName (_.Id) { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                }
            ]
            testList "partialById" [
                testTask "succeeds when a document is updated" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
                    
                    do! Update.partialById Db.tableName "one" {| NumValue = 44 |}
                    let! after = Find.byId<string, JsonDocument> Db.tableName "one"
                    if Option.isNone after then
                        Expect.isTrue false "There should have been a document returned post-update"
                    Expect.equal after.Value.NumValue 44 "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use! db = Db.buildDb ()
        
                    let! before = Find.all<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.partialById Db.tableName "test" {| Foo = "green" |}
                }
            ]
            testList "partialByField" [
                testTask "succeeds when a document is updated" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
                    
                    do! Update.partialByField Db.tableName "Value" EQ "purple" {| NumValue = 77 |}
                    let! after = Count.byField Db.tableName "NumValue" EQ 77
                    Expect.equal after 2L "There should have been 2 documents returned"
                }
                testTask "succeeds when no document is updated" {
                    use! db = Db.buildDb ()
        
                    let! before = Find.all<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! Update.partialByField Db.tableName "Value" EQ "burgundy" {| Foo = "green" |}
                }
            ]
        ]
        testList "Delete" [
            testList "byId" [
                testTask "succeeds when a document is deleted" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    do! Delete.byId Db.tableName "four"
                    let! remaining = Count.all Db.tableName
                    Expect.equal remaining 4L "There should have been 4 documents remaining"
                }
                testTask "succeeds when a document is not deleted" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    do! Delete.byId Db.tableName "thirty"
                    let! remaining = Count.all Db.tableName
                    Expect.equal remaining 5L "There should have been 5 documents remaining"
                }
            ]
            testList "byField" [
                testTask "succeeds when documents are deleted" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    do! Delete.byField Db.tableName "Value" NE "purple"
                    let! remaining = Count.all Db.tableName
                    Expect.equal remaining 2L "There should have been 2 documents remaining"
                }
                testTask "succeeds when documents are not deleted" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    do! Delete.byField Db.tableName "Value" EQ "crimson"
                    let! remaining = Count.all Db.tableName
                    Expect.equal remaining 5L "There should have been 5 documents remaining"
                }
            ]
        ]
        testList "Custom" [
            testList "single" [
                testTask "succeeds when a row is found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc =
                        Custom.single
                            $"SELECT data FROM {Db.tableName} WHERE data ->> 'Id' = @id"
                            [ SqliteParameter("@id", "one") ]
                            fromData<JsonDocument>
                    Expect.isSome doc "There should have been a document returned"
                    Expect.equal doc.Value.Id "one" "The incorrect document was returned"
                }
                testTask "succeeds when a row is not found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! doc =
                        Custom.single
                            $"SELECT data FROM {Db.tableName} WHERE data ->> 'Id' = @id"
                            [ SqliteParameter("@id", "eighty") ]
                            fromData<JsonDocument>
                    Expect.isNone doc "There should not have been a document returned"
                }
            ]
            testList "list" [
                testTask "succeeds when data is found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! docs = Custom.list (Query.selectFromTable Db.tableName) [] fromData<JsonDocument>
                    Expect.hasCountOf docs 5u isTrue "There should have been 5 documents returned"
                }
                testTask "succeeds when data is not found" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()
        
                    let! docs =
                        Custom.list
                            $"SELECT data FROM {Db.tableName} WHERE data ->> 'NumValue' > @value"
                            [ SqliteParameter("@value", 100) ]
                            fromData<JsonDocument>
                    Expect.isEmpty docs "There should have been no documents returned"
                }
            ]
            testList "nonQuery" [
                testTask "succeeds when operating on data" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()

                    do! Custom.nonQuery $"DELETE FROM {Db.tableName}" []

                    let! remaining = Count.all Db.tableName
                    Expect.equal remaining 0L "There should be no documents remaining in the table"
                }
                testTask "succeeds when no data matches where clause" {
                    use! db = Db.buildDb ()
                    do! loadDocs ()

                    do! Custom.nonQuery
                            $"DELETE FROM {Db.tableName} WHERE data ->> 'NumValue' > @value"
                            [ SqliteParameter("@value", 100) ]

                    let! remaining = Count.all Db.tableName
                    Expect.equal remaining 5L "There should be 5 documents remaining in the table"
                }
            ]
            testTask "scalar succeeds" {
                use! db = Db.buildDb ()
        
                let! nbr = Custom.scalar "SELECT 5 AS test_value" [] _.GetInt32(0)
                Expect.equal nbr 5 "The query should have returned the number 5"
            }
        ]
        testList "Extensions" [
            testTask "ensureTable succeeds" {
                use! db   = Db.buildDb ()
                use  conn = Configuration.dbConn ()
                let itExists (name: string) = task {
                    let! result =
                        conn.customScalar
                            $"SELECT EXISTS (SELECT 1 FROM {Db.catalog} WHERE name = @name) AS it"
                            [ SqliteParameter("@name", name) ]
                            _.GetInt64(0)
                    return result > 0
                }
                
                let! exists     = itExists "ensured"
                let! alsoExists = itExists "idx_ensured_key"
                Expect.isFalse exists     "The table should not exist already"
                Expect.isFalse alsoExists "The key index should not exist already"
        
                do! conn.ensureTable "ensured"
                let! exists'     = itExists "ensured"
                let! alsoExists' = itExists "idx_ensured_key"
                Expect.isTrue exists'    "The table should now exist"
                Expect.isTrue alsoExists' "The key index should now exist"
            }
            testList "insert" [
                testTask "succeeds" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    let! before = conn.findAll<SubDocument> Db.tableName
                    Expect.equal before [] "There should be no documents in the table"
            
                    let testDoc = { emptyDoc with Id = "turkey"; Sub = Some { Foo = "gobble"; Bar = "gobble" } }
                    do! conn.insert Db.tableName testDoc
                    let! after = conn.findAll<JsonDocument> Db.tableName
                    Expect.equal after [ testDoc ] "There should have been one document inserted"
                }
                testTask "fails for duplicate key" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! conn.insert Db.tableName { emptyDoc with Id = "test" }
                    Expect.throws
                        (fun () ->
                            conn.insert Db.tableName {emptyDoc with Id = "test" }
                            |> Async.AwaitTask
                            |> Async.RunSynchronously)
                        "An exception should have been raised for duplicate document ID insert"
                }
            ]
            testList "save" [
                testTask "succeeds when a document is inserted" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    let! before = conn.findAll<JsonDocument> Db.tableName
                    Expect.equal before [] "There should be no documents in the table"
            
                    let testDoc = { emptyDoc with Id = "test"; Sub = Some { Foo = "a"; Bar = "b" } }
                    do! conn.save Db.tableName testDoc
                    let! after = conn.findAll<JsonDocument> Db.tableName
                    Expect.equal after [ testDoc ] "There should have been one document inserted"
                }
                testTask "succeeds when a document is updated" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    let testDoc = { emptyDoc with Id = "test"; Sub = Some { Foo = "a"; Bar = "b" } }
                    do! conn.insert Db.tableName testDoc
            
                    let! before = conn.findById<string, JsonDocument> Db.tableName "test"
                    if Option.isNone before then Expect.isTrue false "There should have been a document returned"
                    Expect.equal before.Value testDoc "The document is not correct"
            
                    let upd8Doc = { testDoc with Sub = Some { Foo = "c"; Bar = "d" } }
                    do! conn.save Db.tableName upd8Doc
                    let! after = conn.findById<string, JsonDocument> Db.tableName "test"
                    if Option.isNone after then
                        Expect.isTrue false "There should have been a document returned post-update"
                    Expect.equal after.Value upd8Doc "The updated document is not correct"
                }
            ]
            testTask "countAll succeeds" {
                use! db   = Db.buildDb ()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
        
                let! theCount = conn.countAll Db.tableName
                Expect.equal theCount 5L "There should have been 5 matching documents"
            }
            testTask "countByField succeeds" {
                use! db   = Db.buildDb ()
                use  conn = Configuration.dbConn ()
                do! loadDocs ()
        
                let! theCount = conn.countByField Db.tableName "Value" EQ "purple"
                Expect.equal theCount 2L "There should have been 2 matching documents"
            }
            testList "existsById" [
                testTask "succeeds when a document exists" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! exists = conn.existsById Db.tableName "three"
                    Expect.isTrue exists "There should have been an existing document"
                }
                testTask "succeeds when a document does not exist" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! exists = conn.existsById Db.tableName "seven"
                    Expect.isFalse exists "There should not have been an existing document"
                }
            ]
            testList "existsByField" [
                testTask "succeeds when documents exist" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! exists = conn.existsByField Db.tableName "NumValue" EQ 10
                    Expect.isTrue exists "There should have been existing documents"
                }
                testTask "succeeds when no matching documents exist" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! exists = conn.existsByField Db.tableName "Nothing" EQ "none"
                    Expect.isFalse exists "There should not have been any existing documents"
                }
            ]
            testList "findAll" [
                testTask "succeeds when there is data" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
        
                    do! insert Db.tableName { Foo = "one"; Bar = "two" }
                    do! insert Db.tableName { Foo = "three"; Bar = "four" }
                    do! insert Db.tableName { Foo = "five"; Bar = "six" }
        
                    let! results = conn.findAll<SubDocument> Db.tableName
                    let expected = [
                        { Foo = "one"; Bar = "two" }
                        { Foo = "three"; Bar = "four" }
                        { Foo = "five"; Bar = "six" }
                    ]
                    Expect.equal results expected "There should have been 3 documents returned"
                }
                testTask "succeeds when there is no data" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    let! results = conn.findAll<SubDocument> Db.tableName
                    Expect.equal results [] "There should have been no documents returned"
                }
            ]
            testList "findById" [
                testTask "succeeds when a document is found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! doc = conn.findById<string, JsonDocument> Db.tableName "two"
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.equal doc.Value.Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! doc = conn.findById<string, JsonDocument> Db.tableName "three hundred eighty-seven"
                    Expect.isFalse (Option.isSome doc) "There should not have been a document returned"
                }
            ]
            testList "findByField" [
                testTask "succeeds when documents are found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! docs = conn.findByField<JsonDocument> Db.tableName "Sub.Foo" EQ "green"
                    Expect.equal (List.length docs) 2 "There should have been two documents returned"
                }
                testTask "succeeds when documents are not found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! docs = conn.findByField<JsonDocument> Db.tableName "Value" EQ "mauve"
                    Expect.isTrue (List.isEmpty docs) "There should have been no documents returned"
                }
            ]
            testList "findFirstByField" [
                testTask "succeeds when a document is found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! doc = conn.findFirstByField<JsonDocument> Db.tableName "Value" EQ "another"
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.equal doc.Value.Id "two" "The incorrect document was returned"
                }
                testTask "succeeds when multiple documents are found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! doc = conn.findFirstByField<JsonDocument> Db.tableName "Sub.Foo" EQ "green"
                    Expect.isTrue (Option.isSome doc) "There should have been a document returned"
                    Expect.contains [ "two"; "four" ] doc.Value.Id "An incorrect document was returned"
                }
                testTask "succeeds when a document is not found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! doc = conn.findFirstByField<JsonDocument> Db.tableName "Value" EQ "absent"
                    Expect.isFalse (Option.isSome doc) "There should not have been a document returned"
                }
            ]
            testList "updateFull" [
                testTask "succeeds when a document is updated" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let testDoc = { emptyDoc with Id = "one"; Sub = Some { Foo = "blue"; Bar = "red" } }
                    do! conn.updateFull Db.tableName "one" testDoc
                    let! after = conn.findById<string, JsonDocument> Db.tableName "one"
                    if Option.isNone after then
                        Expect.isTrue false "There should have been a document returned post-update"
                    Expect.equal after.Value testDoc "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
        
                    let! before = conn.findAll<JsonDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! conn.updateFull
                            Db.tableName
                            "test"
                            { emptyDoc with Id = "x"; Sub = Some { Foo = "blue"; Bar = "red" } }
                }
            ]
            testList "updateFullFunc" [
                testTask "succeeds when a document is updated" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    do! conn.updateFullFunc
                            Db.tableName
                            (_.Id)
                            { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                    let! after = conn.findById<string, JsonDocument> Db.tableName "one"
                    if Option.isNone after then
                        Expect.isTrue false "There should have been a document returned post-update"
                    Expect.equal
                        after.Value
                        { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                        "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
        
                    let! before = conn.findAll<JsonDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! conn.updateFullFunc
                            Db.tableName
                            (_.Id)
                            { Id = "one"; Value = "le un"; NumValue = 1; Sub = None }
                }
            ]
            testList "updatePartialById" [
                testTask "succeeds when a document is updated" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
                    
                    do! conn.updatePartialById Db.tableName "one" {| NumValue = 44 |}
                    let! after = conn.findById<string, JsonDocument> Db.tableName "one"
                    if Option.isNone after then
                        Expect.isTrue false "There should have been a document returned post-update"
                    Expect.equal after.Value.NumValue 44 "The updated document is not correct"
                }
                testTask "succeeds when no document is updated" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
        
                    let! before = conn.findAll<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! conn.updatePartialById Db.tableName "test" {| Foo = "green" |}
                }
            ]
            testList "updatePartialByField" [
                testTask "succeeds when a document is updated" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
                    
                    do! conn.updatePartialByField Db.tableName "Value" EQ "purple" {| NumValue = 77 |}
                    let! after = conn.countByField Db.tableName "NumValue" EQ 77
                    Expect.equal after 2L "There should have been 2 documents returned"
                }
                testTask "succeeds when no document is updated" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
        
                    let! before = conn.findAll<SubDocument> Db.tableName
                    Expect.hasCountOf before 0u isTrue "There should have been no documents returned"
                    
                    // This not raising an exception is the test
                    do! conn.updatePartialByField Db.tableName "Value" EQ "burgundy" {| Foo = "green" |}
                }
            ]
            testList "deleteById" [
                testTask "succeeds when a document is deleted" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    do! conn.deleteById Db.tableName "four"
                    let! remaining = conn.countAll Db.tableName
                    Expect.equal remaining 4L "There should have been 4 documents remaining"
                }
                testTask "succeeds when a document is not deleted" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    do! conn.deleteById Db.tableName "thirty"
                    let! remaining = conn.countAll Db.tableName
                    Expect.equal remaining 5L "There should have been 5 documents remaining"
                }
            ]
            testList "deleteByField" [
                testTask "succeeds when documents are deleted" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    do! conn.deleteByField Db.tableName "Value" NE "purple"
                    let! remaining = conn.countAll Db.tableName
                    Expect.equal remaining 2L "There should have been 2 documents remaining"
                }
                testTask "succeeds when documents are not deleted" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    do! conn.deleteByField Db.tableName "Value" EQ "crimson"
                    let! remaining = conn.countAll Db.tableName
                    Expect.equal remaining 5L "There should have been 5 documents remaining"
                }
            ]
            testList "customSingle" [
                testTask "succeeds when a row is found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! doc =
                        conn.customSingle
                            $"SELECT data FROM {Db.tableName} WHERE data ->> 'Id' = @id"
                            [ SqliteParameter("@id", "one") ]
                            fromData<JsonDocument>
                    Expect.isSome doc "There should have been a document returned"
                    Expect.equal doc.Value.Id "one" "The incorrect document was returned"
                }
                testTask "succeeds when a row is not found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! doc =
                        conn.customSingle
                            $"SELECT data FROM {Db.tableName} WHERE data ->> 'Id' = @id"
                            [ SqliteParameter("@id", "eighty") ]
                            fromData<JsonDocument>
                    Expect.isNone doc "There should not have been a document returned"
                }
            ]
            testList "customList" [
                testTask "succeeds when data is found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! docs = conn.customList (Query.selectFromTable Db.tableName) [] fromData<JsonDocument>
                    Expect.hasCountOf docs 5u isTrue "There should have been 5 documents returned"
                }
                testTask "succeeds when data is not found" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()
        
                    let! docs =
                        conn.customList
                            $"SELECT data FROM {Db.tableName} WHERE data ->> 'NumValue' > @value"
                            [ SqliteParameter("@value", 100) ]
                            fromData<JsonDocument>
                    Expect.isEmpty docs "There should have been no documents returned"
                }
            ]
            testList "customNonQuery" [
                testTask "succeeds when operating on data" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()

                    do! conn.customNonQuery $"DELETE FROM {Db.tableName}" []

                    let! remaining = conn.countAll Db.tableName
                    Expect.equal remaining 0L "There should be no documents remaining in the table"
                }
                testTask "succeeds when no data matches where clause" {
                    use! db   = Db.buildDb ()
                    use  conn = Configuration.dbConn ()
                    do! loadDocs ()

                    do! conn.customNonQuery
                            $"DELETE FROM {Db.tableName} WHERE data ->> 'NumValue' > @value"
                            [ SqliteParameter("@value", 100) ]

                    let! remaining = conn.countAll Db.tableName
                    Expect.equal remaining 5L "There should be 5 documents remaining in the table"
                }
            ]
            testTask "customScalar succeeds" {
                use! db   = Db.buildDb ()
                use  conn = Configuration.dbConn ()
        
                let! nbr = conn.customScalar "SELECT 5 AS test_value" [] _.GetInt32(0)
                Expect.equal nbr 5 "The query should have returned the number 5"
            }
        ]
        test "clean up database" {
            Configuration.useConnectionString "data source=:memory:"
        }
    ]
    |> testSequenced

let all = testList "FSharp.Documents" [ unitTests; integrationTests ]
