/**
 * End-to-end testing that the search index commands work on both mongos and mongod.
 */

(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");

const dbName = jsTestName();
const collName = "testColl";

let unavailableHostAndPort;

// Test the mock search index management server works.
{
    const mongotMock = new MongotMock();
    mongotMock.start();

    const testResponse = {
        someField: 'someFieldValue',
    };

    // Set up a mock response to the 'manageSearchIndex' command.
    mongotMock.setMockSearchIndexCommandResponse(testResponse);

    // Call the 'manageSearchIndex' command and check it returns the response that was set.
    const response = mongotMock.callManageSearchIndexCommand();
    // The response includes ok:1, so check the field specifically.
    assert.eq(testResponse.someField, response.someField);

    // Save this for later testing of an unavailable remote server.
    unavailableHostAndPort = mongotMock.getConnection().host;

    mongotMock.stop();
}

// Test that the search index commands are not supported if the 'searchIndexManagementHostAndPort'
// server parameter is not set. Initializing the server parameter conveys that the server is running
// with search index management and the search index commands are supported.
{
    const runHostAndPortNotSetTest = function(conn) {
        const testDB = conn.getDB(dbName);
        assert.commandFailedWithCode(testDB.runCommand({
            'createSearchIndexes': collName,
            'indexes': [{'definition': {'mappings': {'dynamic': true}}}]
        }),
                                     ErrorCodes.CommandNotSupported);

        assert.commandFailedWithCode(testDB.runCommand({
            'updateSearchIndex': collName,
            'id': 'index-ID-number',
            'definition': {"testBlob": "blob"}
        }),
                                     ErrorCodes.CommandNotSupported);

        assert.commandFailedWithCode(
            testDB.runCommand({'dropSearchIndex': collName, 'name': 'indexName'}),
            ErrorCodes.CommandNotSupported);

        assert.commandFailedWithCode(testDB.runCommand({'listSearchIndexes': collName}),
                                     ErrorCodes.CommandNotSupported);
    };
    let st = new ShardingTest({
        mongos: 1,
        shards: 1,
    });
    // Test the mongos search index commands.
    runHostAndPortNotSetTest(st.s);
    // Test the mongod search index commands.
    runHostAndPortNotSetTest(st.shard0);
    st.stop();
}

// Test that the mongod search index commands fail when the remote search index management server is
// not reachable. Set a host-and-port for the remote server that is not live in order to simulate
// unreachability.
{
    const runHostAndPortUnreachableTest = function(conn) {
        const testDB = conn.getDB(dbName);
        assert.commandFailedWithCode(testDB.runCommand({
            'createSearchIndexes': collName,
            'indexes': [{'definition': {'mappings': {'dynamic': true}}}]
        }),
                                     ErrorCodes.CommandFailed);

        // The code to reach the remote search index management server is shared across search index
        // commands. No need to test all of the commands.
    };

    let st = new ShardingTest({
        mongos: 1,
        shards: 1,
        other: {
            mongosOptions:
                {setParameter: {searchIndexManagementHostAndPort: unavailableHostAndPort}},
            shardOptions:
                {setParameter: {searchIndexManagementHostAndPort: unavailableHostAndPort}},
        }
    });
    const mongos = st.s;
    const testDBMongos = mongos.getDB(dbName);
    const testCollMongos = testDBMongos.getCollection(collName);

    // Create and shard the test collection so the commands can succeed locally.
    assert.commandWorked(testDBMongos.createCollection(collName));
    assert.commandWorked(mongos.adminCommand({enableSharding: dbName}));
    assert.commandWorked(
        mongos.adminCommand({shardCollection: testCollMongos.getFullName(), key: {a: 1}}));

    runHostAndPortUnreachableTest(mongos);
    runHostAndPortUnreachableTest(st.shard0);

    st.stop();
}

const mongotMock = new MongotMock();
mongotMock.start();
const mockConn = mongotMock.getConnection();

// Test that the mongod links in the mongod-only command logic, not the mongos logic that asks the
// config server for the collection UUID.
{
    const conn =
        MongoRunner.runMongod({setParameter: {searchIndexManagementHostAndPort: mockConn.host}});
    assert(conn);
    const testDB = conn.getDB(dbName);

    // Create the collection so the commands can succeed on the mongod.
    assert.commandWorked(testDB.createCollection(collName));

    const manageSearchIndexCommandResponse = {
        indexesCreated: [{id: "index-Id", name: "index-name"}]
    };

    mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
    assert.commandWorked(testDB.runCommand({
        'createSearchIndexes': collName,
        'indexes': [{'definition': {'mappings': {'dynamic': true}}}]
    }));

    MongoRunner.stopMongod(conn);
}

let st = new ShardingTest({
    mongos: 1,
    shards: 1,
    other: {
        mongosOptions: {setParameter: {searchIndexManagementHostAndPort: mockConn.host}},
        rs0: {
            // Need the shard to have a stable secondary to test commands against.
            nodes: [{}, {rsConfig: {priority: 0}}],
            setParameter: {searchIndexManagementHostAndPort: mockConn.host},
        },
    }
});

const mongos = st.s;
const testDBMongos = mongos.getDB(dbName);
const testCollMongos = testDBMongos.getCollection(collName);

// Test that the commands all fail if the collection does not exist
{
    const runCollectionDoesNotExistTest = function(conn) {
        const testDB = conn.getDB(dbName);

        assert.commandFailedWithCode(testDB.runCommand({
            'createSearchIndexes': collName,
            'indexes': [{'definition': {'mappings': {'dynamic': true}}}]
        }),
                                     ErrorCodes.NamespaceNotFound);

        assert.commandFailedWithCode(testDB.runCommand({
            'updateSearchIndex': collName,
            'id': 'index-ID-number',
            'definition': {"testBlob": "blob"}
        }),
                                     ErrorCodes.NamespaceNotFound);

        assert.commandFailedWithCode(
            testDB.runCommand({'dropSearchIndex': collName, 'name': 'indexName'}),
            ErrorCodes.NamespaceNotFound);

        assert.commandFailedWithCode(testDB.runCommand({'listSearchIndexes': collName}),
                                     ErrorCodes.NamespaceNotFound);
    };
    runCollectionDoesNotExistTest(st.s);
    runCollectionDoesNotExistTest(st.shard0);
}

// Create and shard the collection so the commands can succeed.
assert.commandWorked(testDBMongos.createCollection(collName));
assert.commandWorked(mongos.adminCommand({enableSharding: dbName}));
assert.commandWorked(
    mongos.adminCommand({shardCollection: testCollMongos.getFullName(), key: {a: 1}}));

// Test that the search index commands fail against a secondary replica set member.
{
    const secondaryDB = st.rs0.getSecondary().getDB(dbName);

    assert.commandFailedWithCode(secondaryDB.runCommand({
        'createSearchIndexes': collName,
        'indexes': [{'definition': {'mappings': {'dynamic': true}}}]
    }),
                                 ErrorCodes.NotWritablePrimary);

    assert.commandFailedWithCode(secondaryDB.runCommand({
        'updateSearchIndex': collName,
        'id': 'index-ID-number',
        'definition': {"testBlob": "blob"}
    }),
                                 ErrorCodes.NotWritablePrimary);

    assert.commandFailedWithCode(
        secondaryDB.runCommand({'dropSearchIndex': collName, 'name': 'indexName'}),
        ErrorCodes.NotWritablePrimary);

    assert.commandFailedWithCode(secondaryDB.runCommand({'listSearchIndexes': collName}),
                                 ErrorCodes.NotWritablePrimary);
}

// Test creating search indexes.
{
    const runCreateSearchIndexesTest = function(conn) {
        const testDB = conn.getDB(dbName);
        const manageSearchIndexCommandResponse = {
            indexesCreated: [{id: "index-Id", name: "index-name"}]
        };

        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
        assert.commandWorked(testDB.runCommand({
            'createSearchIndexes': collName,
            'indexes': [{'definition': {'mappings': {'dynamic': true}}}]
        }));

        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
        assert.commandWorked(testDB.runCommand({
            'createSearchIndexes': collName,
            'indexes': [{'name': 'indexName', 'definition': {'mappings': {'dynamic': true}}}]
        }));

        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
        assert.commandWorked(testDB.runCommand({
            'createSearchIndexes': collName,
            'indexes': [
                {'name': 'indexName', 'definition': {'mappings': {'dynamic': true}}},
                {'definition': {'mappings': {'dynamic': false}}},
            ]
        }));
    };
    runCreateSearchIndexesTest(mongos);
    runCreateSearchIndexesTest(st.shard0);
}

// Test updating search indexes.
{
    const runUpdateSearchIndexTest = function(conn) {
        const testDB = conn.getDB(dbName);
        const manageSearchIndexCommandResponse = {ok: 1};

        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
        assert.commandWorked(testDB.runCommand({
            'updateSearchIndex': collName,
            'id': 'index-ID-number',
            'definition': {"testBlob": "blob"}
        }));

        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
        assert.commandWorked(testDB.runCommand({
            'updateSearchIndex': collName,
            'name': 'indexName',
            'definition': {"testBlob": "blob"}
        }));

        // Cannot run update without specifying what index to update by 'name' or 'id'.
        assert.commandFailedWithCode(
            testDB.runCommand({'updateSearchIndex': collName, 'definition': {"testBlob": "blob"}}),
            ErrorCodes.InvalidOptions);

        // Not allowed to run update specifying both 'name' and 'id'.
        assert.commandFailedWithCode(testDB.runCommand({
            'updateSearchIndex': collName,
            'name': 'indexName',
            'id': 'index-ID-number',
            'definition': {"testBlob": "blob"}
        }),
                                     ErrorCodes.InvalidOptions);
    };
    runUpdateSearchIndexTest(st.s);
    runUpdateSearchIndexTest(st.shard0);
}

// Test dropping search indexes.
{
    const runDropSearchIndexTest = function(conn) {
        const testDB = conn.getDB(dbName);
        const manageSearchIndexCommandResponse = {ok: 1};

        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
        assert.commandWorked(testDB.runCommand({'dropSearchIndex': collName, 'name': 'indexName'}));

        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
        assert.commandWorked(
            testDB.runCommand({'dropSearchIndex': collName, 'id': 'index-ID-number'}));

        // Not allowed to run drop specifying both 'name' and 'id'.
        assert.commandFailedWithCode(
            testDB.runCommand(
                {'dropSearchIndex': collName, 'name': 'indexName', 'id': 'index-ID-number'}),
            ErrorCodes.InvalidOptions);
    };
    runDropSearchIndexTest(st.s);
    runDropSearchIndexTest(st.shard0);
}

// Test listing the search indexes.
{
    const runListSearchIndexesTest = function(conn) {
        const testDB = conn.getDB(dbName);

        const manageSearchIndexCommandResponse = {
            ok: 1,
            cursor: {
                id: 0,
                ns: "database-name.collection-name",
                firstBatch: [
                    {
                        id: "index-Id",
                        name: "index-name",
                        status: "INITIAL-SYNC",
                        definition: {
                            mappings: {
                                dynamic: true,
                            }
                        },
                    },
                    {
                        id: "index-Id",
                        name: "index-name",
                        status: "ACTIVE",
                        definition: {
                            mappings: {
                                dynamic: true,
                            },
                            synonyms: [{"synonym-mapping": "thing"}],
                        }
                    }
                ]
            }
        };

        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
        assert.commandWorked(testDB.runCommand({'listSearchIndexes': collName}));

        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
        assert.commandWorked(
            testDB.runCommand({'listSearchIndexes': collName, 'name': 'indexName'}));

        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
        assert.commandWorked(
            testDB.runCommand({'listSearchIndexes': collName, 'id': 'index-ID-number'}));

        // Not allowed to run list specifying both 'name' and 'id'.
        assert.commandFailedWithCode(
            testDB.runCommand(
                {'listSearchIndexes': collName, 'name': 'indexName', 'id': 'index-ID-number'}),
            ErrorCodes.InvalidOptions);
    };
    runListSearchIndexesTest(st.s);
    runListSearchIndexesTest(st.shard0);
}

// Test that a search index management server error propagates back through the mongod correctly.
{
    const runRemoteSearchIndexManagementServerErrorsTest = function(conn) {
        const testDB = conn.getDB(dbName);
        const manageSearchIndexCommandResponse = {
            ok: 0,
            errmsg: "create failed due to malformed index (pretend)",
            code: 207,
            // Choose a different error than what the search commands can return to ensure the error
            // gets passed through.
            codeName: "InvalidUUID",
        };

        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
        let res = assert.commandFailedWithCode(testDB.runCommand({
            'createSearchIndexes': collName,
            'indexes': [
                {'name': 'indexName', 'definition': {'mappings': {'dynamic': true}}},
                {'definition': {'mappings': {'dynamic': false}}},
            ],
        }),
                                               ErrorCodes.InvalidUUID);
        delete res.$clusterTime;
        delete res.operationTime;
        assert.eq(manageSearchIndexCommandResponse, res);

        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
        res = assert.commandFailedWithCode(
            testDB.runCommand({'dropSearchIndex': collName, 'name': 'indexName'}),
            ErrorCodes.InvalidUUID);
        delete res.$clusterTime;
        delete res.operationTime;
        assert.eq(manageSearchIndexCommandResponse, res);

        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
        res = assert.commandFailedWithCode(testDB.runCommand({
            'updateSearchIndex': collName,
            'id': 'index-ID-number',
            'definition': {"testBlob": "blob"},
        }),
                                           ErrorCodes.InvalidUUID);
        delete res.$clusterTime;
        delete res.operationTime;
        assert.eq(manageSearchIndexCommandResponse, res);

        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
        res = assert.commandFailedWithCode(testDB.runCommand({'listSearchIndexes': collName}),
                                           ErrorCodes.InvalidUUID);
        delete res.$clusterTime;
        delete res.operationTime;
        assert.eq(manageSearchIndexCommandResponse, res);

        // Exercise returning a IndexInformationTooLarge error that only the remote search index
        // management server generates.
        const manageSearchIndexListIndexesResponse = {
            ok: 0,
            errmsg: "the search index information for this collection exceeds 16 MB",
            code: 396,
            codeName: "IndexInformationTooLarge",
        };
        mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexListIndexesResponse);
        res = assert.commandFailedWithCode(testDB.runCommand({'listSearchIndexes': collName}),
                                           ErrorCodes.IndexInformationTooLarge);
        delete res.$clusterTime;
        delete res.operationTime;
        assert.eq(manageSearchIndexListIndexesResponse, res);
    };
    runRemoteSearchIndexManagementServerErrorsTest(mongos);
    runRemoteSearchIndexManagementServerErrorsTest(st.shard0);
}

st.stop();
mongotMock.stop();
}());
