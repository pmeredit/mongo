/**
 * Test encrypted delete works under a user txn.
 *
 * @tags: [
 *  featureFlagFLE2,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

if (!isFLE2ShardingEnabled()) {
    return;
}

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

assert.commandWorked(sessionColl.insert({"first": "mark", "last": "marco"}));
assert.commandWorked(sessionColl.insert({"first": "Mark", "last": "Marco"}));
assert.commandWorked(sessionColl.deleteOne({"last": "marco"}));

session.commitTransaction();

client.assertEncryptedCollectionCounts("basic", 1, 2, 1, 3);

// Verify we insert two documents in a txn but abort it
session.startTransaction();

assert.commandWorked(sessionColl.deleteOne({"last": "Marco"}));

// In the TXN the counts are right
client.assertEncryptedCollectionCounts("basic", 0, 2, 2, 4);

assert.commandWorked(session.abortTransaction_forTesting());

// Then they revert after it is aborted
client.assertEncryptedCollectionCounts("basic", 1, 2, 1, 3);
}());
