/**
 * Test getQueryableEncryptionCountInfo handles incorrect collections
 *
 * @tags: [
 * assumes_standalone_mongod,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

if (!isFLE2ProtocolVersion2Enabled()) {
    jsTest.log("Test skipped because featureFlagFLE2ProtocolVersion2 is enabled");
    return;
}

let dbName = 'get_tags';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

// Trigger the database creation
assert.commandWorked(dbTest.foo.insert({_id: "ignore"}));

// Test against an non-existent collection
// Note: the tokens are simply random test values.
assert.commandWorked(dbTest.runCommand({
    getQueryableEncryptionCountInfo: "does_not_exist",
    tokens: [
        {tokens: [{"s": BinData(0, "lUBO7Mov5Sb+c/D4cJ9whhhw/+PZFLCk/AQU2+BpumQ=")}]},
    ],
    "forInsert": true,
}));

assert.commandWorked(dbTest.testColl.insert({_id: "bogus"}));

// Find a short base64 value
assert.commandFailed(dbTest.runCommand({
    getQueryableEncryptionCountInfo: "testColl",
    tokens: [
        {tokens: [{"s": BinData(0, "MTIzNA==")}]},
    ],
    "forInsert": true,
}));

// Find a tag with a bogus value
assert.commandWorked(dbTest.runCommand({
    getQueryableEncryptionCountInfo: "testColl",
    tokens: [
        {tokens: [{"s": BinData(0, "ABCO7Mov5Sb+c/D4cJ9whhhw/+PZFLCk/AQU2+BpumQ=")}]},
    ],
    "forInsert": true,
}));
}());
