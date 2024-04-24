/**
 * Test encrypted find and modify works with array filters
 *
 * @tags: [
 * assumes_unsharded_collection,
 * requires_fcv_70
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

const dbName = 'find_and_modify_array_filters';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

const edb = client.getDB();
edb.basic.einsertOne({"_id": 1, "first": "mark", "grades": [95, 92, 90]});
edb.basic.einsertOne({"_id": 2, "first": "mark", "grades": [98, 100, 102]});
edb.basic.einsertOne({"_id": 3, "first": "mark", "grades": [95, 110, 100]});

print("EDC: " + tojson(dbTest.basic.find().toArray()));
client.assertEncryptedCollectionCounts("basic", 3, 3, 3);

// Update a document by array filters
client.assertDocumentChanges("basic", [0, 2], [1], () => {
    assert.commandWorked(edb.basic.erunCommand({
        findAndModify: edb.basic.getName(),
        query: {grades: {$gte: 100}},
        update: {$set: {"grades.$[element]": 100}},
        arrayFilters: [{"element": {$gte: 100}}]
    }));
});

const doc = edb.basic.findOne({_id: 2});
assert.eq(doc["grades"], [98, 100, 100]);

client.assertEncryptedCollectionCounts("basic", 3, 3, 3);
