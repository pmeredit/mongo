/**
 * Test encrypted update works under a user txn.
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
assert.commandWorked(sessionColl.updateOne({"last": "marco"}, {$set: {"first": "matthew"}}));

session.commitTransaction();

client.assertEncryptedCollectionCounts("basic", 2, 3, 1, 4);

// Verify we insert two documents in a txn but abort it
session.startTransaction();

assert.commandWorked(sessionColl.updateOne({"last": "Marco"}, {$set: {"first": "Matthew"}}));

// In the TXN the counts are right
client.assertEncryptedCollectionCounts("basic", 2, 4, 2, 6);

assert.commandWorked(session.abortTransaction_forTesting());

// Then they revert after it is aborted
client.assertEncryptedCollectionCounts("basic", 2, 3, 1, 4);
}());
