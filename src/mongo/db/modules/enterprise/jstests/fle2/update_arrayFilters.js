/**
 * Test encrypted update with array filters works
 *
 * @tags: [
 * assumes_unsharded_collection,
 * requires_fcv_70
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

let dbName = 'update_arrayFilters';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

let edb = client.getDB();
edb.basic.einsertOne({"_id": 1, "first": "mark", "grades": [95, 92, 90]});
edb.basic.einsertOne({"_id": 2, "first": "mark", "grades": [98, 100, 102]});
edb.basic.einsertOne({"_id": 3, "first": "mark", "grades": [95, 110, 100]});

client.assertEncryptedCollectionCounts("basic", 3, 3, 3);

client.assertOneEncryptedDocumentFields("basic", {"_id": 1}, {"first": "mark"});

let res = client.assertDocumentChanges("basic", [0, 2], [1], () => {
    return edb.basic.eupdateOne({grades: {$gte: 100}},
                                {$set: {"grades.$[element]": 100}},
                                {arrayFilters: [{"element": {$gte: 100}}]});
});
assert.eq(res.modifiedCount, 1);

let doc = edb.basic.efindOne({_id: 2});
assert.eq(doc["grades"], [98, 100, 100]);

client.assertEncryptedCollectionCounts("basic", 3, 3, 3);
