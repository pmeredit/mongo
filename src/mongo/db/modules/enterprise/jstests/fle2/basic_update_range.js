/**
 * Test encrypted update works
 *
 * @tags: [
 *   featureFlagFLE2Range,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

// TODO SERVER-67760 remove once feature flag is gone
if (!isFLE2RangeEnabled()) {
    jsTest.log("Test skipped because featureFlagFLE2Range is not enabled");
    return;
}

let dbName = 'basic_insert_range';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

const edb = client.getDB();

jsTest.log("createEncryptionCollection");
assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields: {
        "fields": [
            {
                "path": "age",  // first name
                "bsonType": "int",
                "queries": {
                    "queryType": "rangePreview",
                    "min": NumberInt(1),
                    "max": NumberInt(16),
                    "sparsity": 1
                }
            },
            {
                path: "name",
                bsonType: "string",
            }
        ]
    }
}));

assert.commandWorked(edb.basic.insert({"id": 1, "name": "Bob", "age": NumberInt(12)}));
assert.commandWorked(edb.basic.insert({"id": 2, "name": "Linda"}));

const kHypergraphHeight = 5;

client.assertEncryptedCollectionCounts("basic", 2, kHypergraphHeight, 0, kHypergraphHeight);

assert.commandWorked(edb.basic.runCommand(
    {update: edb.basic.getName(), updates: [{q: {"id": 1}, u: {"$set": {"age": NumberInt(8)}}}]}));

assert.commandWorked(edb.basic.runCommand(
    {update: edb.basic.getName(), updates: [{q: {"id": 2}, u: {"$set": {"age": NumberInt(5)}}}]}));

client.assertEncryptedCollectionCounts(
    "basic", 2, 3 * kHypergraphHeight, kHypergraphHeight, 4 * kHypergraphHeight);

assert.commandWorked(edb.basic.runCommand(
    {update: edb.basic.getName(), updates: [{q: {"id": 1}, u: {"$unset": {"age": ""}}}]}));

client.assertEncryptedCollectionCounts(
    "basic", 2, 3 * kHypergraphHeight, 2 * kHypergraphHeight, 5 * kHypergraphHeight);
}());
