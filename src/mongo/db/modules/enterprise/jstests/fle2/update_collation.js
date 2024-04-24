/**
 * Test encrypted update works with collation.
 *
 * @tags: [
 * assumes_unsharded_collection,
 * requires_fcv_70
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

let dbName = 'update_collation';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

let edb = client.getDB();
assert.commandWorked(edb.basic.einsert({"first": "mark", "last": "Markus"}));
assert.commandWorked(edb.basic.einsert({"first": "Mark", "last": "Marco"}));

print("EDC: " + tojson(dbTest.basic.find().toArray()));
client.assertEncryptedCollectionCounts("basic", 2, 2, 2);

// Update a document by case-insensitive collation
let res = assert.commandWorked(edb.basic.eupdateOne(
    {"last": "marco"}, {$set: {"first": "matthew"}}, {collation: {locale: 'en_US', strength: 2}}));
assert.eq(res.modifiedCount, 1);

client.assertOneEncryptedDocumentFields("basic", {"last": "Marco"}, {"first": "matthew"});

client.assertEncryptedCollectionCounts("basic", 2, 3, 3);
