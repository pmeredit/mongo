/**
 * Test FLE Range CRUD ops on Decimal 128
 * Decimal 128 is its own encoding, so it demands its own test.
 *
 * @tags: [
 * requires_fcv_80,
 * ]
 */

import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

let dbName = 'basic_decimal128_range';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

jsTest.log("Creating Encrypted Collection");

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields: {
        "fields": [
            {
                "path": "length",
                "bsonType": "decimal",
                "queries": {"queryType": "range", "sparsity": 1}
            },
            {
                "path": "width",
                "bsonType": "int",
                "queries": {
                    "queryType": "equality",
                    "contention": 0,
                }
            },
            {
                "path": "height",
                "bsonType": "decimal",
            }
        ]
    }
}));

const kLEdgesGeneratedPerOp = 129;  // Number of edges generated for length
const kWEdgesGeneratedPerOp = 1;    // Number of edges generated for width

const edb = client.getDB();
assert.commandWorked(edb.basic.insert({
    "last": "square1",
    "length": NumberDecimal(3.1415),
    "width": NumberInt(1),
    "height": NumberDecimal(10.325)
}));
assert.commandWorked(edb.basic.insert(
    {"last": "square2", "length": NumberDecimal(1.3543), "height": NumberDecimal(22.493)}));
assert.commandWorked(
    edb.basic.insert({"last": "square3", "width": NumberInt(5), "height": NumberDecimal(193.50)}));
assert.commandWorked(edb.basic.insert({
    "last": "square4",
    "length": NumberDecimal(9.7923),
    "width": NumberInt(2),
}));

let currentESCCount = 0;
let currentECOCCount = 0;

const edgesForInserts = 3 * kLEdgesGeneratedPerOp + 3 * kWEdgesGeneratedPerOp;

currentESCCount = currentECOCCount = edgesForInserts;

client.assertEncryptedCollectionCounts("basic", 4, currentESCCount, currentECOCCount);

assert.commandWorked(edb.runCommand({
    findAndModify: edb.basic.getName(),
    "query": {"last": "square1"},
    "update": {"$set": {"length": NumberDecimal(9.213)}}
}));

currentESCCount += kLEdgesGeneratedPerOp;
currentECOCCount += kLEdgesGeneratedPerOp;

client.assertEncryptedCollectionCounts("basic", 4, currentESCCount, currentECOCCount);

assert.commandWorked(edb.runCommand({
    update: edb.basic.getName(),
    updates: [{"q": {"last": "square2"}, "u": {"$set": {"width": NumberInt(5)}}}],
}));

currentESCCount += kWEdgesGeneratedPerOp;
currentECOCCount += kWEdgesGeneratedPerOp;

client.assertEncryptedCollectionCounts("basic", 4, currentESCCount, currentECOCCount);

assert.commandWorked(edb.basic.deleteOne({"last": "square4"}));

client.assertEncryptedCollectionCounts("basic", 3, currentESCCount, currentECOCCount);
