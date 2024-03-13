/**
 * Test encrypted delete works
 *
 * @tags: [
 *   requires_fcv_80,
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

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
                "queries":
                    {"queryType": "range", "min": NumberInt(1), "max": NumberInt(16), "sparsity": 1}
            },
            {
                path: "name",
                bsonType: "string",
            }
        ]
    }
}));

assert.commandWorked(edb.basic.insert({"name": "Bob", "age": NumberInt(12), "last": "Belcher"}));

const kHypergraphHeight = 5;

client.assertEncryptedCollectionCounts("basic", 1, kHypergraphHeight, kHypergraphHeight);

assert.commandWorked(edb.basic.deleteOne({"last": "Belcher"}));

client.assertEncryptedCollectionCounts("basic", 0, kHypergraphHeight, kHypergraphHeight);
