/**
 * Test that fle2 is non-deterministic.
 *
 * @tags: [
 * requires_fcv_70
 * ]
 */

load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

const dbName = 'fle2_non_deterministic';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);

jsTest.log("Ensuring that two BinData with the same information are equal.");

const bin1Test = BinData(6, "data");
const bin2Test = BinData(6, "data");

assert.eq(bin1Test, bin2Test);

assert.commandWorked(client.createEncryptionCollection("basic1", {
    encryptedFields: {
        "fields": [
            {"path": "field1", "bsonType": "string", "queries": {"queryType": "equality"}},
            {
                "path": "field2",
                "bsonType": "string",
            }
        ]
    }
}));

assert.commandWorked(client.createEncryptionCollection("basic2", {
    encryptedFields: {
        "fields": [
            {"path": "field1", "bsonType": "string", "queries": {"queryType": "equality"}},
            {
                "path": "field2",
                "bsonType": "string",
            }
        ]
    }
}));

const edb = client.getDB();

const doc = {
    "_id": 1,
    "field1": "foo",
    "field2": "bar"
};

assert.commandWorked(edb.basic1.insert(doc));
assert.commandWorked(edb.basic2.insert(doc));

const unencryptedDbTest = db.getSiblingDB(dbName);

jsTest.log("Ensuring that two items with the same value encrypted by fle2 are different BinData.");

const basic1Doc = unencryptedDbTest.basic1.find({_id: 1}).toArray()[0];
const basic2Doc = unencryptedDbTest.basic2.find({_id: 1}).toArray()[0];

assert.neq(basic1Doc.field1, basic2Doc.field1);
assert.neq(basic1Doc.field2, basic2Doc.field2);
})();