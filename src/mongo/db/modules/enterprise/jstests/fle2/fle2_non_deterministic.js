/**
 * Test that fle2 is non-deterministic.
 *
 * @tags: [
 * requires_fcv_70
 * ]
 */

import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

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

assert.commandWorked(edb.basic1.einsert(doc));
assert.commandWorked(edb.basic2.einsert(doc));

const unencryptedDbTest = db.getSiblingDB(dbName);

jsTest.log("Ensuring that two items with the same value encrypted by fle2 are different BinData.");

const basic1Doc = unencryptedDbTest.basic1.findOne({_id: 1});
const basic2Doc = unencryptedDbTest.basic2.findOne({_id: 1});

assert.neq(basic1Doc.field1, basic2Doc.field1);
assert.neq(basic1Doc.field2, basic2Doc.field2);