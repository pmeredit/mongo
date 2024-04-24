/**
 * Test encrypted find and modify works
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

assert.commandWorked(edb.basic.einsert({"last": "Belcher", "name": "Bob", "age": NumberInt(12)}));
assert.commandWorked(edb.basic.einsert({"last": "Stotch", "name": "Linda"}));

const kHypergraphHeight = 5;

client.assertEncryptedCollectionCounts("basic", 2, kHypergraphHeight, kHypergraphHeight);

assert.commandWorked(edb.basic.erunCommand({
    findAndModify: edb.basic.getName(),
    "query": {"last": "Belcher"},
    "update": {"$set": {"age": NumberInt(8)}}
}));
assert.commandWorked(edb.basic.erunCommand({
    findAndModify: edb.basic.getName(),
    "query": {"last": "Stotch"},
    "update": {"$set": {"age": NumberInt(5)}}
}));

client.assertEncryptedCollectionCounts("basic", 2, 3 * kHypergraphHeight, 3 * kHypergraphHeight);

assert.commandWorked(edb.basic.erunCommand({
    findAndModify: edb.basic.getName(),
    "query": {"last": "Belcher"},
    "update": {"$unset": {"age": ""}}
}));

client.assertEncryptedCollectionCounts("basic", 2, 3 * kHypergraphHeight, 3 * kHypergraphHeight);
