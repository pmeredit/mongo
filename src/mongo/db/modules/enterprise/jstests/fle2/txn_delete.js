/**
 * Test encrypted delete works under a user txn.
 *
 * @tags: [
 * does_not_support_causal_consistency,
 * assumes_read_concern_unchanged,
 * assumes_unsharded_collection
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

let dbName = 'txn_delete';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

// TODO: SERVER-73303 remove when v2 is enabled by default & update ECOC expected counts
if (isFLE2ProtocolVersion2Enabled()) {
    client.ecocCountMatchesEscCount = true;
}

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

assert.commandWorked(sessionColl.insert({"first": "mark", "last": "marco"}));
assert.commandWorked(sessionColl.insert({"first": "Mark", "last": "Marco"}));
assert.commandWorked(sessionColl.deleteOne({"last": "marco"}));

session.commitTransaction();

client.assertEncryptedCollectionCounts("basic", 1, 2, 1, 3);

// Verify we delete two documents in a txn but abort it
session.startTransaction();

assert.commandWorked(sessionColl.deleteOne({"last": "Marco"}));

// In the TXN the counts are right
client.assertEncryptedCollectionCountsByObject(sessionDB, "basic", 0, 2, 2, 4);

assert.commandWorked(session.abortTransaction_forTesting());

// Then they revert after it is aborted
client.assertEncryptedCollectionCounts("basic", 1, 2, 1, 3);

session.startTransaction();

// Verify we delete a document by an encrypted field but abort it.
assert.commandWorked(sessionColl.deleteOne({"first": "Mark"}));

// In the TXN the counts are right
client.assertEncryptedCollectionCountsByObject(sessionDB, "basic", 0, 2, 2, 4);

assert.commandWorked(session.abortTransaction_forTesting());

// Then they revert after it is aborted
client.assertEncryptedCollectionCounts("basic", 1, 2, 1, 3);

// TODO: SERVER-73303 remove when v2 is enabled by default
if (!isFLE2ProtocolVersion2Enabled()) {
    // v1 does not support multi-document deletes tested below
    return;
}

// Insert new documents for testing multi-document deletes
session.startTransaction();
assert.commandWorked(sessionColl.insert({"first": "george", "last": "washington"}));
assert.commandWorked(sessionColl.insert({"first": "george", "last": "foreman"}));
assert.commandWorked(sessionColl.insert({"first": "michael", "last": "scott"}));
assert.commandWorked(sessionColl.insert({"first": "michael", "last": "jackson"}));
assert.commandWorked(sessionColl.deleteMany({"last": "Marco"}));
session.commitTransaction();
client.assertEncryptedCollectionCounts("basic", 4, 6, 0, 6);

// Verify we can do multi-document deletes in a txn
session.startTransaction();
assert.commandWorked(sessionColl.deleteMany({"first": "george"}));
session.commitTransaction();
client.assertEncryptedCollectionCounts("basic", 2, 6, 0, 6);

// Verify we can abort multi-document deletes
session.startTransaction();
assert.commandWorked(sessionColl.deleteMany({"first": "michael"}));
// In the TXN the counts are right
client.assertEncryptedCollectionCountsByObject(sessionDB, "basic", 0, 6, 0, 6);
assert.commandWorked(session.abortTransaction_forTesting());
// Then they revert after it is aborted
client.assertEncryptedCollectionCounts("basic", 2, 6, 0, 6);
}());
