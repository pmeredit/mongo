/**
 * Test encrypted update works under a user txn.
 *
 * @tags: [
 * does_not_support_causal_consistency,
 * assumes_read_concern_unchanged,
 * assumes_unsharded_collection,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

let dbName = 'txn_update';
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

dbTest.basic.createIndex({"middle": 1}, {unique: true});

// Insert a document with a field that gets encrypted
const session = edb.getMongo().startSession({causalConsistency: false});
const sessionDB = session.getDatabase(dbName);
const sessionColl = sessionDB.getCollection("basic");

// Verify we can insert two documents in a txn
session.startTransaction();

assert.commandWorked(
    sessionColl.insert({_id: 1, "first": "mark", "last": "marco", "middle": "matthew"}));
assert.commandWorked(
    sessionColl.insert({_id: 2, "first": "Mark", "last": "Marco", "middle": "Matthew"}));
assert.commandWorked(sessionColl.updateOne({"last": "marco"}, {$set: {"first": "matthew"}}));

session.commitTransaction();

client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "matthew", "last": "marco", "middle": "matthew"},
    {"_id": 2, "first": "Mark", "last": "Marco", "middle": "Matthew"},
]);

client.assertEncryptedCollectionCounts("basic", 2, 3, 1, 4);

// Verify we insert two documents in a txn but abort it
session.startTransaction();

assert.commandWorked(sessionColl.updateOne({"last": "Marco"}, {$set: {"first": "Matthew"}}));

// In the TXN the counts are right
client.assertEncryptedCollectionCountsByObject(sessionDB, "basic", 2, 4, 2, 6);

assert.commandWorked(session.abortTransaction_forTesting());

// Then they revert after it is aborted
client.assertEncryptedCollectionCounts("basic", 2, 3, 1, 4);

//////////////////////////////////////////////////////////////////////////////////////////////////////
// Verify we can update documents while querying by an encrypted field and abort the transaction.
session.startTransaction();
assert.commandWorked(sessionColl.updateOne({"first": "Mark"}, {$set: {"first": "Matthew"}}));
// In the TXN the counts are right
client.assertEncryptedCollectionCountsByObject(sessionDB, "basic", 2, 4, 2, 6);
assert.commandWorked(session.abortTransaction_forTesting());
// Then they revert after it is aborted
client.assertEncryptedCollectionCounts("basic", 2, 3, 1, 4);

// Verify we can update documents while querying by an encrypted field and commit the transaction.
session.startTransaction();
assert.commandWorked(sessionColl.updateOne({"first": "Mark"}, {$set: {"first": "Matthew"}}));
client.assertEncryptedCollectionCountsByObject(sessionDB, "basic", 2, 4, 2, 6);
session.commitTransaction();
// Counts should persist outside the transaction.
client.assertEncryptedCollectionCounts("basic", 2, 4, 2, 6);

//////////////////////////////////////////////////////////////////////////////////////////////////////
// Verify we can abort a txn with an error

session.startTransaction();

let res = assert.commandFailed(sessionColl.runCommand({
    update: edb.basic.getName(),
    updates: [{q: {"last": "marco"}, u: {$set: {"middle": "Matthew"}}}]
}));
print(tojson(res));
assert.eq(res.writeErrors[0].code, 11000);

client.assertEncryptedCollectionCounts("basic", 2, 4, 2, 6);
}());
