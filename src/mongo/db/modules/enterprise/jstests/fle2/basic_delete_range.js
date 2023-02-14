/**
 * Test encrypted delete works
 *
 * @tags: [
 *   requires_fcv_62,
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

assert.commandWorked(edb.basic.insert({"name": "Bob", "age": NumberInt(12)}));

const kHypergraphHeight = 5;

client.assertEncryptedCollectionCounts("basic", 1, kHypergraphHeight, 0, kHypergraphHeight);

let doc = edb.basic.find().toArray()[0];

assert.commandWorked(edb.basic.deleteOne({"_id": doc._id}));

// TODO: SERVER-73303 remove else branch once v2 is enabled by default
if (isFLE2ProtocolVersion2Enabled()) {
    client.assertEncryptedCollectionCounts("basic", 0, kHypergraphHeight, 0, kHypergraphHeight);
} else {
    client.assertEncryptedCollectionCounts(
        "basic", 0, kHypergraphHeight, kHypergraphHeight, 2 * kHypergraphHeight);
}
}());
