/**
 * Test encrypted insert works under a user txn.
 *
 * @tags: [
 * assumes_read_concern_unchanged,
 * assumes_read_preference_unchanged,
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
                    "min": NumberLong(0),
                    "max": NumberLong(7),
                    "sparsity": 1,
                }
            },
        ]
    }
}));

let edb = client.getDB();

// Insert a document with a field that gets encrypted
const session = edb.getMongo().startSession({causalConsistency: false});
const sessionDB = session.getDatabase(dbName);
const sessionColl = sessionDB.getCollection("basic");

// Verify we can insert two documents in a txn
session.startTransaction();

assert.commandWorked(sessionColl.einsert({"height": NumberLong(4)}));
assert.commandWorked(sessionColl.einsert({"height": NumberLong(5)}));

session.commitTransaction();

client.assertEncryptedCollectionCounts("basic", 2, 8, 8);

// Verify we insert two documents in a txn but abort it
session.startTransaction();

assert.commandWorked(sessionColl.einsert({"height": NumberLong(1)}));
assert.commandWorked(sessionColl.einsert({"height": NumberLong(2)}));

assert.commandWorked(session.abortTransaction_forTesting());

client.assertEncryptedCollectionCounts("basic", 2, 8, 8);

// Verify it aborts cleanly with an unrecoverable error
session.startTransaction();

assert.commandWorked(sessionColl.einsert({"_id": 1, "height": NumberLong(7)}));
let res = assert.commandFailedWithCode(sessionColl.einsert({"_id": 1, "height": NumberLong(3)}),
                                       ErrorCodes.DuplicateKey);

// DuplicateKey is not a transient error.
assert.eq(res.errorLabels, null);

client.assertEncryptedCollectionCounts("basic", 2, 8, 8);
