/**
 * Test encrypted delete works under a user txn.
 *
 * @tags: [
 * does_not_support_causal_consistency,
 * assumes_read_concern_unchanged,
 * assumes_unsharded_collection,
 * requires_fcv_70,
 * uses_transactions,
 * # TODO SERVER-87046: re-enable test in suites with random migrations
 * assumes_balancer_off,
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

let dbName = 'txn_delete';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

let edb = client.getDB();

// Insert a document with a field that gets encrypted
const session = edb.getMongo().startSession({causalConsistency: false});
const sessionDB = session.getDatabase(dbName);
const sessionColl = sessionDB.getCollection("basic");

// Verify we can insert two documents in a txn
session.startTransaction();

assert.commandWorked(sessionColl.einsert({"first": "mark", "last": "marco"}));
assert.commandWorked(sessionColl.einsert({"first": "Mark", "last": "Marco"}));
assert.commandWorked(sessionColl.edeleteOne({"last": "marco"}));

session.commitTransaction();

client.assertEncryptedCollectionCounts("basic", 1, 2, 2);

// Verify we delete two documents in a txn but abort it
session.startTransaction();

assert.commandWorked(sessionColl.edeleteOne({"last": "Marco"}));

// In the TXN the counts are right
client.assertEncryptedCollectionCountsByObject(sessionDB, "basic", 0, 2, 2);

assert.commandWorked(session.abortTransaction_forTesting());

// Then they revert after it is aborted
client.assertEncryptedCollectionCounts("basic", 1, 2, 2);

session.startTransaction();

// Verify we delete a document by an encrypted field but abort it.
assert.commandWorked(sessionColl.edeleteOne({"first": "Mark"}));

// In the TXN the counts are right
client.assertEncryptedCollectionCountsByObject(sessionDB, "basic", 0, 2, 2);

assert.commandWorked(session.abortTransaction_forTesting());

// Then they revert after it is aborted
client.assertEncryptedCollectionCounts("basic", 1, 2, 2);

// Insert new documents for testing multi-document deletes
session.startTransaction();

assert.commandWorked(sessionColl.einsert({"first": "george", "last": "washington"}));
assert.commandWorked(sessionColl.einsert({"first": "george", "last": "foreman"}));
assert.commandWorked(sessionColl.einsert({"first": "michael", "last": "scott"}));
assert.commandWorked(sessionColl.einsert({"first": "michael", "last": "jackson"}));
assert.commandWorked(sessionColl.edeleteMany({"last": "Marco"}));
session.commitTransaction();
client.assertEncryptedCollectionCounts("basic", 4, 6, 6);

// Verify we can do multi-document deletes in a txn
session.startTransaction();

assert.commandWorked(sessionColl.edeleteMany({"first": "george"}));
session.commitTransaction();
client.assertEncryptedCollectionCounts("basic", 2, 6, 6);

// Verify we can abort multi-document deletes
session.startTransaction();

assert.commandWorked(sessionColl.edeleteMany({"first": "michael"}));
// In the TXN the counts are right
client.assertEncryptedCollectionCountsByObject(sessionDB, "basic", 0, 6, 6);
assert.commandWorked(session.abortTransaction_forTesting());
// Then they revert after it is aborted
client.assertEncryptedCollectionCounts("basic", 2, 6, 6);
