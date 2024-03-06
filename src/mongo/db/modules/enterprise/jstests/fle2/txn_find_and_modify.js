/**
 * Test encrypted find and modify works under a user txn.
 *
 * @tags: [
 * does_not_support_causal_consistency,
 * assumes_read_concern_unchanged,
 * assumes_unsharded_collection,
 * requires_fcv_70,
 * uses_transactions,
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

let dbName = 'txn_find_and_modify';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

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
    sessionColl.insert({"_id": 1, "first": "mark", "last": "marco", "middle": "marky"}));
assert.commandWorked(
    sessionColl.insert({"_id": 2, "first": "Mark", "last": "Marco", "middle": "Marky"}));
let res = assert.commandWorked(sessionColl.runCommand({
    findAndModify: edb.basic.getName(),
    query: {"last": "marco"},
    update: {$set: {"first": "matthew"}}
}));
print("RES:" + tojson(res));

session.commitTransaction();

client.assertEncryptedCollectionCounts("basic", 2, 3, 3);

client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "matthew", "last": "marco", "middle": "marky"},
    {"_id": 2, "first": "Mark", "last": "Marco", "middle": "Marky"},
]);

//////////////////////////////////////////////////////////////////////////////////
// Verify we insert two documents in a txn but abort it
session.startTransaction();

assert.commandWorked(sessionColl.runCommand({
    findAndModify: edb.basic.getName(),
    query: {"last": "Marco"},
    update: {$set: {"first": "Matthew"}}
}));

// In the TXN the counts are right
client.assertEncryptedCollectionCountsByObject(sessionDB, "basic", 2, 4, 4);

assert.commandWorked(session.abortTransaction_forTesting());

// Then they revert after it is aborted
client.assertEncryptedCollectionCounts("basic", 2, 3, 3);

//////////////////////////////////////////////////////////////////////////////////////////////////////
// Verify we can abort a txn with an error

session.startTransaction();

res = assert.commandFailed(sessionColl.runCommand({
    findAndModify: edb.basic.getName(),
    query: {"last": "marco"},
    update: {$set: {"middle": "Marky"}}
}));
print(tojson(res));
assert.eq(res.code, ErrorCodes.DuplicateKey);
assert.eq(res.codeName, "DuplicateKey");
assert(res.hasOwnProperty("errmsg"));

client.assertEncryptedCollectionCounts("basic", 2, 3, 3);
