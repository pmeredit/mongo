/**
 * Test encrypted update with array filters works
 *
 * @tags: [
 * assumes_unsharded_collection,
 * requires_fcv_70
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

let dbName = 'update_arrayFilters';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

let edb = client.getDB();
edb.basic.insertOne({"_id": 1, "first": "mark", "grades": [95, 92, 90]});
edb.basic.insertOne({"_id": 2, "first": "mark", "grades": [98, 100, 102]});
edb.basic.insertOne({"_id": 3, "first": "mark", "grades": [95, 110, 100]});

client.assertEncryptedCollectionCounts("basic", 3, 3, 0, 3);

client.assertOneEncryptedDocumentFields("basic", {"_id": 1}, {"first": "mark"});

let res = client.assertDocumentChanges("basic", [0, 2], [1], () => {
    return edb.basic.updateOne({grades: {$gte: 100}},
                               {$set: {"grades.$[element]": 100}},
                               {arrayFilters: [{"element": {$gte: 100}}]});
});
assert.eq(res.modifiedCount, 1);

let doc = edb.basic.find({_id: 2}).toArray()[0];
assert.eq(doc["grades"], [98, 100, 100]);

client.assertEncryptedCollectionCounts("basic", 3, 3, 0, 3);
}());
