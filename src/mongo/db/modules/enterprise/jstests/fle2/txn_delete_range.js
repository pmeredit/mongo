/**
 * Test encrypted delete works under a user txn.
 *
 * @tags: [
 * assumes_read_concern_unchanged,
 * assumes_read_preference_unchanged,
 * assumes_unsharded_collection,
 * requires_fcv_80,
 * uses_transactions,
 * # TODO SERVER-87046: re-enable test in suites with random migrations
 * assumes_balancer_off,
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

let dbName = 'txn_insert_range';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields: {
        "fields": [
            {
                "path": "height",
                "bsonType": "long",
                "queries": {
                    "queryType": "range",
                    "min": NumberLong(1),
                    "max": NumberLong(16),
                    "sparsity": 1,
                }
            },
            {
                "path": "num.num",
                "bsonType": "int",
                "queries":
                    {"queryType": "range", "min": NumberInt(0), "max": NumberInt(3), "sparsity": 2}
            },
            {"path": "ssn", "bsonType": "string", "queries": {"queryType": "equality"}}
        ]
    }
}));

let edb = client.getDB();

// Insert a document with a field that gets encrypted
const session = edb.getMongo().startSession({causalConsistency: false});
const sessionDB = session.getDatabase(dbName);
const sessionColl = sessionDB.getCollection("basic");

const kHypergraphHeight = 5;

// Hypergraph for numnum has height of 2 because sparsity is 2
const kHypergraphNumNum = 2;
const kEqualityTags = 1;
const kTagsPerEntry = kHypergraphHeight + kHypergraphNumNum + kEqualityTags;

// Verify we can insert two documents in a txn
session.startTransaction();

assert.commandWorked(sessionColl.insert(
    {name: "joe", "height": NumberLong(4), "num": {"num": NumberInt(2)}, "ssn": "abcd"}));
assert.commandWorked(sessionColl.insert(
    {name: "bob", "height": NumberLong(5), "num": {"num": NumberInt(1)}, "ssn": "efgh"}));

session.commitTransaction();

client.assertEncryptedCollectionCounts("basic", 2, 2 * kTagsPerEntry, 2 * kTagsPerEntry);

session.startTransaction();

assert.commandWorked(sessionColl.deleteOne({name: "joe"}));
assert.commandWorked(sessionColl.deleteOne({name: "bob"}));

assert.commandWorked(session.abortTransaction_forTesting());

client.assertEncryptedCollectionCounts("basic", 2, 2 * kTagsPerEntry, 2 * kTagsPerEntry);

session.startTransaction();

assert.commandWorked(sessionColl.deleteOne({name: "joe"}));
assert.commandWorked(sessionColl.deleteOne({name: "bob"}));

session.commitTransaction();

client.assertEncryptedCollectionCounts("basic", 0, 2 * kTagsPerEntry, 2 * kTagsPerEntry);
