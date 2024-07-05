/**
 * Test encrypted update works
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

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields: {
        "fields": [
            {
                "path": "age",
                "bsonType": "int",
                "queries": {
                    "queryType": "range",
                    "min": NumberInt(1),
                    "max": NumberInt(16),
                    "sparsity": 1,
                    "trimFactor": 0
                }
            },
            {
                "path": "rating",
                "bsonType": "int",
                "queries": {"queryType": "range", "sparsity": 1, "trimFactor": 0}
            },
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
    update: edb.basic.getName(),
    updates: [{q: {"last": "Belcher"}, u: {"$set": {"age": NumberInt(8)}}}]
}));

assert.commandWorked(edb.basic.erunCommand({
    update: edb.basic.getName(),
    updates: [{q: {"last": "Stotch"}, u: {"$set": {"age": NumberInt(5)}}}]
}));
totalEdges += (kAgeHypergraphHeight * 2);

client.assertEncryptedCollectionCounts("basic", 3, totalEdges, totalEdges);

assert.commandWorked(edb.basic.erunCommand({
    update: edb.basic.getName(),
    updates: [{q: {"last": "Belcher"}, u: {"$unset": {"age": ""}}}]
}));

client.assertEncryptedCollectionCounts("basic", 3, totalEdges, totalEdges);

assert.commandWorked(edb.basic.erunCommand({
    update: edb.basic.getName(),
    updates: [{q: {"last": "Pesto"}, u: {"$set": {"rating": NumberInt(-2147483648)}}}]
}));
assert.commandWorked(edb.basic.erunCommand({
    update: edb.basic.getName(),
    updates: [{q: {"last": "Belcher"}, u: {"$set": {"rating": NumberInt(-2147483648)}}}]
}));

totalEdges += (kRatingHypergraphHeight * 2);
client.assertEncryptedCollectionCounts("basic", 3, totalEdges, totalEdges);
