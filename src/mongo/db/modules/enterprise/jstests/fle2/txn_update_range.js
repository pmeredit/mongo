/**
 * Test encrypted update works under a user txn.
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

const edb = client.getDB();

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

assert.commandWorked(sessionColl.einsert(
    {name: "joe", "height": NumberLong(4), "num": {"num": NumberInt(2)}, "ssn": "abcd"}));
assert.commandWorked(sessionColl.einsert(
    {name: "bob", "height": NumberLong(5), "num": {"num": NumberInt(1)}, "ssn": "efgh"}));

session.commitTransaction();

client.assertEncryptedCollectionCounts("basic", 2, 2 * kTagsPerEntry, 2 * kTagsPerEntry);

session.startTransaction();

assert.commandWorked(sessionColl.erunCommand({
    update: edb.basic.getName(),
    updates: [{"q": {name: "joe"}, "u": {"$set": {"num": {"num": NumberInt(0)}}}}]
}));
assert.commandWorked(sessionColl.erunCommand({
    update: edb.basic.getName(),
    updates: [{"q": {name: "bob"}, "u": {"$set": {"height": NumberLong(6)}}}]
}));

assert.commandWorked(session.abortTransaction_forTesting());

client.assertEncryptedCollectionCounts("basic", 2, 2 * kTagsPerEntry, 2 * kTagsPerEntry);

session.startTransaction();

// Replace both the documents, same value, but replace all the fields.
assert.commandWorked(sessionColl.erunCommand({
    update: edb.basic.getName(),
    updates: [{
        "q": {name: "joe"},
        "u": {name: "john", "height": NumberLong(4), "num": {"num": NumberInt(2)}, "ssn": "abcd"}
    }]
}));
assert.commandWorked(sessionColl.erunCommand({
    update: edb.basic.getName(),
    updates: [{
        "q": {name: "bob"},
        "u": {name: "billy", "height": NumberLong(4), "num": {"num": NumberInt(2)}, "ssn": "abcd"}
    }]
}));

session.commitTransaction();

client.assertEncryptedCollectionCounts("basic", 2, 4 * kTagsPerEntry, 4 * kTagsPerEntry);

session.startTransaction();

assert.commandWorked(sessionColl.erunCommand({
    update: edb.basic.getName(),
    updates: [{"q": {name: "john"}, "u": {"$set": {"num": {"num": NumberInt(0)}}}}]
}));
assert.commandWorked(sessionColl.erunCommand({
    update: edb.basic.getName(),
    updates: [{"q": {name: "billy"}, "u": {"$set": {"height": NumberLong(6)}}}]
}));

session.commitTransaction();

const numChangedLastSession = /* We add 2 for the change for num.num */ kHypergraphNumNum +
    /* We add 5 for the change to height */ kHypergraphHeight;

const escCount = 4 * kTagsPerEntry + numChangedLastSession;
const ecocCount = 4 * kTagsPerEntry + numChangedLastSession;

client.assertEncryptedCollectionCounts("basic", 2, escCount, ecocCount);
