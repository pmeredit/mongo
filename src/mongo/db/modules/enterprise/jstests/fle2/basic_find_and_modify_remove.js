/**
 * Test encrypted find and modify with remove works
 *
 * @tags: [
 *   assumes_read_preference_unchanged,
 *   requires_fcv_70
 * ]
 */
import {isMongos} from "jstests/concurrency/fsm_workload_helpers/server_types.js";
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

const dbName = 'basic_find_and_modify_remove';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

const edb = client.getDB();
assert.commandWorked(
    edb.basic.einsert({"_id": 1, "first": "mark", "last": "marco", "middle": "markus"}));
assert.commandWorked(
    edb.basic.einsert({"_id": 2, "first": "Mark", "last": "Marcus", "middle": "markus"}));

print("EDC: " + tojson(dbTest.basic.find().toArray()));
client.assertEncryptedCollectionCounts("basic", 2, 2, 2);

client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "mark"});

//////////////////////////////////////////////////////////////////////////////////////////////////////
// Remove a document
assert.commandWorked(edb.basic.erunCommand(
    {findAndModify: edb.basic.getName(), query: {"last": "marco"}, remove: true}));

client.assertEncryptedCollectionCounts("basic", 1, 2, 2);

client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 2, "first": "Mark", "last": "Marcus", "middle": "markus"},
]);

if (!client.useImplicitSharding) {
    //////////////////////////////////////////////////////////////////////////////////////////////////////
    // Remove a document by collation
    assert.commandWorked(edb.basic.erunCommand({
        findAndModify: edb.basic.getName(),
        query: {"last": "marcus"},
        remove: true,
        collation: {locale: 'en_US', strength: 2}
    }));

    client.assertEncryptedCollectionCounts("basic", 0, 2, 2);

    //////////////////////////////////////////////////////////////////////////////////////////////////////
    // FAIL: Remove and update the encrypted field

    // The FLE sharding code throws this directly. In replica sets, the regular mongod code throws
    // this with a different error code
    if (isMongos(db)) {
        assert.commandFailedWithCode(edb.basic.erunCommand({
            findAndModify: edb.basic.getName(),
            query: {"last": "marco"},
            remove: true,
            update: {$set: {"first": "luke"}}
        }),
                                     6371401);
    }
}
