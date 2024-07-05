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

assert.commandWorked(edb.basic.einsert(
    {"name": "Bob", "age": NumberInt(12), "rating": NumberInt(-2147483648), "last": "Belcher"}));
totalEdges += kAgeHypergraphHeight + kRatingHypergraphHeight;
client.assertEncryptedCollectionCounts("basic", 1, totalEdges, totalEdges);

assert.commandWorked(edb.basic.deleteOne({"last": "Belcher"}));

client.assertEncryptedCollectionCounts("basic", 0, totalEdges, totalEdges);
