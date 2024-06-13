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
            {"path": "rating", "bsonType": "int", "queries": {"queryType": "range"}},
            {
                path: "name",
                bsonType: "string",
            }
        ]
    }
}));

const kAgeHypergraphHeight = 5;
const kRatingHypergraphHeight = 33;
let totalEdges = 0;

assert.commandWorked(edb.basic.einsert({"last": "Belcher", "name": "Bob", "age": NumberInt(12)}));
assert.commandWorked(edb.basic.einsert({"last": "Stotch", "name": "Linda"}));
totalEdges += kAgeHypergraphHeight;
client.assertEncryptedCollectionCounts("basic", 2, totalEdges, totalEdges);

assert.commandWorked(edb.basic.einsert({"last": "Pesto", "rating": NumberInt(2147483647)}));
totalEdges += kRatingHypergraphHeight;
client.assertEncryptedCollectionCounts("basic", 3, totalEdges, totalEdges);

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
totalEdges += (kAgeHypergraphHeight * 2);

client.assertEncryptedCollectionCounts("basic", 3, totalEdges, totalEdges);

assert.commandWorked(edb.basic.erunCommand({
    findAndModify: edb.basic.getName(),
    "query": {"last": "Belcher"},
    "update": {"$unset": {"age": ""}}
}));

client.assertEncryptedCollectionCounts("basic", 3, totalEdges, totalEdges);

assert.commandWorked(edb.basic.erunCommand({
    findAndModify: edb.basic.getName(),
    "query": {"last": "Pesto"},
    "update": {"$set": {"rating": NumberInt(-2147483648)}}
}));
assert.commandWorked(edb.basic.erunCommand({
    findAndModify: edb.basic.getName(),
    "query": {"last": "Belcher"},
    "update": {"$set": {"rating": NumberInt(-2147483648)}}
}));

totalEdges += (kRatingHypergraphHeight * 2);
client.assertEncryptedCollectionCounts("basic", 3, totalEdges, totalEdges);
