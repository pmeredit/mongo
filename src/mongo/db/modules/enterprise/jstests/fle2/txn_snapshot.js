/**
 * Test only snapshot transactions work on sharding due to SERVER-77506
 *
 * @tags: [
 * assumes_read_concern_unchanged,
 * directly_against_shardsvrs_incompatible,
 * uses_transactions,
 * ]
 */
import {
    EncryptedClient,
} from "jstests/fle2/libs/encrypted_client_util.js";

function assertCommand(db, result) {
    if (isMongos(db)) {
        assert.commandFailedWithCode(result, 7885501);
    } else {
        assert.commandWorked(result);
    }
}

let dbName = 'txn_snapshot';
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

assert.commandWorked(
    sessionColl.insert({_id: 1, "first": "mark", "last": "marco", "middle": "matthew"}));

// Validate we can run QE in a snapshot transaction regardless of mongod or mongos
//
session.startTransaction({readConcern: {level: "snapshot"}});

assert.commandWorked(
    sessionColl.insert({_id: 2, "first": "Mark", "last": "Marco", "middle": "Matthew"}));
assert.commandWorked(sessionColl.updateOne({"last": "marco"}, {$set: {"first": "matthew"}}));

assert.commandWorked(sessionColl.runCommand({
    findAndModify: edb.basic.getName(),
    query: {"last": "Marco"},
    update: {$set: {"first": "Luke"}}
}));

session.commitTransaction();

client.assertEncryptedCollectionCounts("basic", 2, 4, 4);

client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "matthew", "last": "marco", "middle": "matthew"},
    {"_id": 2, "first": "Luke", "last": "Marco", "middle": "Matthew"},
]);

// Now verify other types of readConcern settings in transactions are not acceptable on shards but
// ok for replica sets
//
const levels = [undefined, "local", "majority"];
let count = 3;

for (let level of levels) {
    jsTestLog(`Txn Test: ReadConcern Level: ${level}`);

    if (level === undefined) {
        session.startTransaction();
    } else {
        session.startTransaction({readConcern: {level: level}});
    }

    count++;

    assertCommand(db,
                  sessionColl.insert(
                      {_id: count, "first": "Mark", "last": "Marco", "middle": "Matthew" + count}));

    assertCommand(db, sessionColl.runCommand({
        update: edb.basic.getName(),
        updates: [{q: {"last": "marco"}, u: {$set: {"first": "matthew"}}}]
    }));

    assertCommand(db, sessionColl.runCommand({
        findAndModify: edb.basic.getName(),
        query: {"last": "marco"},
        update: {$set: {"middle": "Marky"}}
    }));

    session.commitTransaction();
}
