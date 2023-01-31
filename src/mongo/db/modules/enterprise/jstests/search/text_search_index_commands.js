/**
 * Basic end-to-end testing that the search index commands work.
 */

(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");

// Test the mock Atlas endpoint works.
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

const conn =
    MongoRunner.runMongod({setParameter: {searchIndexAtlasHostAndPort: "dummyHostName:20017"}});
assert(conn);
const testDB = conn.getDB("testDatabase");
const testColl = testDB.getCollection("testColl");

assert.commandWorked(testDB.runCommand(
    {'createSearchIndex': testColl.getName(), 'indexDefinition': {"testBlob": "blob"}}));

assert.commandWorked(testDB.runCommand(
    {'dropSearchIndex': testColl.getName(), 'indexDefinition': {"testBlob": "blob"}}));

assert.commandWorked(testDB.runCommand(
    {'modifySearchIndex': testColl.getName(), 'indexDefinition': {"testBlob": "blob"}}));

assert.commandWorked(testDB.runCommand({'listSearchIndexes': testColl.getName()}));

MongoRunner.stopMongod(conn);
}());
