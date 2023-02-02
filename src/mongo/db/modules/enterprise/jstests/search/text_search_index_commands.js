/**
 * Basic end-to-end testing that the search index commands work.
 */

(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");

// Test the mock search index management endpoint works.
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

    mongotMock.stop();
}

// Test that the search index commands are not supported if the 'searchIndexAtlasHostAndPort' server
// parameter is not set. Initializing the server parameter conveys that the server is running with
// search index management and the search index commands are supported.
{
    const conn = MongoRunner.runMongod({});
    assert(conn);
    const testDB = conn.getDB("testDatabase");
    const testColl = testDB.getCollection("testColl");

    assert.commandFailedWithCode(testDB.runCommand({
        'createSearchIndexes': testColl.getName(),
        'indexes': [{'definition': {'mappings': {'dynamic': true}}}]
    }),
                                 ErrorCodes.CommandNotSupported);

    assert.commandFailedWithCode(testDB.runCommand({
        'updateSearchIndex': testColl.getName(),
        'indexID': 'index-ID-number',
        'indexDefinition': {"testBlob": "blob"}
    }),
                                 ErrorCodes.CommandNotSupported);

    assert.commandFailedWithCode(
        testDB.runCommand({'dropSearchIndex': testColl.getName(), 'name': 'indexName'}),
        ErrorCodes.CommandNotSupported);

    assert.commandFailedWithCode(testDB.runCommand({'listSearchIndexes': testColl.getName()}),
                                 ErrorCodes.CommandNotSupported);

    MongoRunner.stopMongod(conn);
}

const mongotMock = new MongotMock();
mongotMock.start();
const mockConn = mongotMock.getConnection();

const conn = MongoRunner.runMongod({setParameter: {searchIndexAtlasHostAndPort: mockConn.host}});
assert(conn);
const testDB = conn.getDB("testDatabase");
const testColl = testDB.getCollection("testColl");

// Test that the commands all fail if the collection does not exist
{
    assert.commandFailedWithCode(testDB.runCommand({
        'createSearchIndexes': testColl.getName(),
        'indexes': [{'definition': {'mappings': {'dynamic': true}}}]
    }),
                                 ErrorCodes.NamespaceNotFound);

    assert.commandFailedWithCode(testDB.runCommand({
        'updateSearchIndex': testColl.getName(),
        'indexID': 'index-ID-number',
        'indexDefinition': {"testBlob": "blob"}
    }),
                                 ErrorCodes.NamespaceNotFound);

    assert.commandFailedWithCode(
        testDB.runCommand({'dropSearchIndex': testColl.getName(), 'name': 'indexName'}),
        ErrorCodes.NamespaceNotFound);

    assert.commandFailedWithCode(testDB.runCommand({'listSearchIndexes': testColl.getName()}),
                                 ErrorCodes.NamespaceNotFound);
}

// Create the collection so the commands can succeed.
assert.commandWorked(testDB.createCollection(testColl.getName()));

// Test creating search indexes.
{
    const manageSearchIndexCommandResponse = {
        indexesCreated: [{indexId: "index-Id", name: "index-name"}]
    };

    mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
    assert.commandWorked(testDB.runCommand({
        'createSearchIndexes': testColl.getName(),
        'indexes': [{'definition': {'mappings': {'dynamic': true}}}]
    }));

    mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
    assert.commandWorked(testDB.runCommand({
        'createSearchIndexes': testColl.getName(),
        'indexes': [{'name': 'indexName', 'definition': {'mappings': {'dynamic': true}}}]
    }));

    mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
    assert.commandWorked(testDB.runCommand({
        'createSearchIndexes': testColl.getName(),
        'indexes': [
            {'name': 'indexName', 'definition': {'mappings': {'dynamic': true}}},
            {'definition': {'mappings': {'dynamic': false}}},
        ]
    }));
}

// Test updating search indexes.
{
    const manageSearchIndexCommandResponse = {ok: 1};

    mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
    assert.commandWorked(testDB.runCommand({
        'updateSearchIndex': testColl.getName(),
        'indexID': 'index-ID-number',
        'indexDefinition': {"testBlob": "blob"}
    }));

    mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
    assert.commandWorked(testDB.runCommand({
        'updateSearchIndex': testColl.getName(),
        'name': 'indexName',
        'indexDefinition': {"testBlob": "blob"}
    }));

    // Cannot run update without specifying what index to update by 'name' or 'indexID'.
    assert.commandFailedWithCode(
        testDB.runCommand(
            {'updateSearchIndex': testColl.getName(), 'indexDefinition': {"testBlob": "blob"}}),
        ErrorCodes.InvalidOptions);

    // Not allowed to run update specifying both 'name' and 'indexID'.
    assert.commandFailedWithCode(testDB.runCommand({
        'updateSearchIndex': testColl.getName(),
        'name': 'indexName',
        'indexID': 'index-ID-number',
        'indexDefinition': {"testBlob": "blob"}
    }),
                                 ErrorCodes.InvalidOptions);
}

// Test dropping search indexes.
{
    const manageSearchIndexCommandResponse = {ok: 1};

    mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
    assert.commandWorked(
        testDB.runCommand({'dropSearchIndex': testColl.getName(), 'name': 'indexName'}));

    mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
    assert.commandWorked(
        testDB.runCommand({'dropSearchIndex': testColl.getName(), 'indexID': 'index-ID-number'}));

    // Not allowed to run drop specifying both 'name' and 'indexID'.
    assert.commandFailedWithCode(testDB.runCommand({
        'dropSearchIndex': testColl.getName(),
        'name': 'indexName',
        'indexID': 'index-ID-number'
    }),
                                 ErrorCodes.InvalidOptions);
}

// Test listing the search indexes.
{
    const manageSearchIndexCommandResponse = {
        ok: 1,
        cursor: {
            id: 0,
            ns: "database-name.collection-name",
            firstBatch: [
                {
                    indexId: "index-Id",
                    name: "index-name",
                    status: "INITIAL-SYNC",
                    indexDefinition: {
                        mappings: {
                            dynamic: true,
                        }
                    },
                },
                {
                    indexId: "index-Id",
                    name: "index-name",
                    status: "ACTIVE",
                    indexDefinition: {
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
    assert.commandWorked(testDB.runCommand({'listSearchIndexes': testColl.getName()}));

    mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
    assert.commandWorked(
        testDB.runCommand({'listSearchIndexes': testColl.getName(), 'name': 'indexName'}));

    mongotMock.setMockSearchIndexCommandResponse(manageSearchIndexCommandResponse);
    assert.commandWorked(
        testDB.runCommand({'listSearchIndexes': testColl.getName(), 'indexID': 'index-ID-number'}));

    // Not allowed to run list specifying both 'name' and 'indexID'.
    assert.commandFailedWithCode(testDB.runCommand({
        'listSearchIndexes': testColl.getName(),
        'name': 'indexName',
        'indexID': 'index-ID-number'
    }),
                                 ErrorCodes.InvalidOptions);
}

MongoRunner.stopMongod(conn);
mongotMock.stop();
}());
