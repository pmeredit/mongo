/**
 * Test double and decimal128 with precision works
 *
 * @tags: [
 *   requires_fcv_62,
 * ]
 */

load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

// TODO: SERVER-73995 remove when v2 collscanmode works
if (isFLE2ProtocolVersion2Enabled() && isFLE2AlwaysUseCollScanModeEnabled(db)) {
    jsTest.log("Test skipped because featureFlagFLE2ProtocolVersion2 and " +
               "internalQueryFLEAlwaysUseEncryptedCollScanMode are enabled");
    return;
}

let dbName = 'basic_range';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);
let edb = client.getDB();

assert.commandWorked(client.createEncryptionCollection("basic_double", {
    encryptedFields: {
        "fields": [
            {
                "path": "count",
                "bsonType": "double",
                "queries": {
                    "queryType": "rangePreview",
                    "sparsity": 1,
                    "min": 0.0,
                    "max": 10.0,
                    "precision": 2
                }
            },
        ]
    }
}));

let res = assert.commandWorked(
    edb.runCommand({"insert": "basic_double", documents: [{"_id": 1, "count": 3.14159}]}));

// Test precision trims the number correctly for $eq
assert.eq(edb.basic_double.find({"count": 3.14159}).itcount(), 1);
assert.eq(edb.basic_double.find({"count": 3.14}).itcount(), 1);
assert.eq(edb.basic_double.find({"count": 3.1}).itcount(), 0);

// Test ranges with trimming
assert.eq(edb.basic_double.find({"count": {$gte: 1.0, $lte: 4.0}}).itcount(), 1);
assert.eq(edb.basic_double.find({"count": {$gte: 1.0, $lte: 3.1}}).itcount(), 0);
assert.eq(edb.basic_double.find({"count": {$gte: 1.0, $lte: 3.14}}).itcount(), 1);
assert.eq(edb.basic_double.find({"count": {$gte: 1.0, $lte: 3.14159}}).itcount(), 1);

assert.commandWorked(client.createEncryptionCollection("basic_decimal", {
    encryptedFields: {
        "fields": [
            {
                "path": "count",
                "bsonType": "decimal",
                "queries": {
                    "queryType": "rangePreview",
                    "sparsity": 1,
                    "min": NumberDecimal(0.0),
                    "max": NumberDecimal(10.0),
                    "precision": 2
                }
            },
        ]
    }
}));

// Insert a document with a field that gets encrypted
res = assert.commandWorked(edb.runCommand(
    {"insert": "basic_decimal", documents: [{"_id": 1, "count": NumberDecimal(3.14159)}]}));

// Test precision trims the number correctly for $eq
assert.eq(edb.basic_decimal.find({"count": NumberDecimal(3.14159)}).itcount(), 1);
assert.eq(edb.basic_decimal.find({"count": NumberDecimal(3.14)}).itcount(), 1);
assert.eq(edb.basic_decimal.find({"count": NumberDecimal(3.1)}).itcount(), 0);

// Test ranges with trimming
assert.eq(edb.basic_decimal.find({"count": {$gte: NumberDecimal(1.0), $lte: NumberDecimal(4.0)}})
              .itcount(),
          1);
assert.eq(edb.basic_decimal.find({"count": {$gte: NumberDecimal(1.0), $lte: NumberDecimal(3.1)}})
              .itcount(),
          0);
assert.eq(edb.basic_decimal.find({"count": {$gte: NumberDecimal(1.0), $lte: NumberDecimal(3.14)}})
              .itcount(),
          1);
assert.eq(
    edb.basic_decimal.find({"count": {$gte: NumberDecimal(1.0), $lte: NumberDecimal(3.14159)}})
        .itcount(),
    1);
}());
