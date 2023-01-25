/**
 * Basic end-to-end testing that the skeleton code for the text-search index commands work.
 *
 */

(function() {
"use strict";

const conn = MongoRunner.runMongod();
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
