/**
 * Test rewrites of agg expressions in update commands over encrypted fields for FLE2.
 *
 * @tags: [
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   assumes_unsharded_collection,
 *   requires_fcv_70,
 * ]
 */

import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {exprTestData} from "src/mongo/db/modules/enterprise/jstests/fle2/query/utils/expr_utils.js";

const {docs, matchFilters} = exprTestData;

// Set up the encrypted collection.
const dbName = "expr_update";
const collName = "updateColl";
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();
let client = new EncryptedClient(db.getMongo(), dbName);
assert.commandWorked(client.createEncryptionCollection(collName, {
    encryptedFields: {
        fields: [
            {path: "ssn", bsonType: "string", queries: {queryType: "equality"}},
            {path: "age", bsonType: "long", queries: {queryType: "equality"}}
        ]
    }

}));
let edb = client.getDB();

const coll = edb[collName];
for (const doc of docs) {
    assert.commandWorked(coll.einsert(doc));
}
assert.commandWorked(coll.createIndex({location: "2dsphere"}));

// Run an update with the provided filter on the provided collection, do a dummy modification, and
// assert the number of modified documents is at least one if expected is non-empty. Multi-document
// updates are not allowed with FLE 2, so we only use updateOne() here.
const runUpdateTest = (filter, collection, expected, extraInfo) => {
    let res = assert.commandWorked(collection.updateOne(filter, {$set: {"updateField": 1}}));
    assert.eq(res.modifiedCount, expected.length > 0 ? 1 : 0, extraInfo);
};

// Run each of the filters above within a find and an aggregate. The results should be consistent.
client.runEncryptionOperation(() => {
    for (const testData of matchFilters) {
        runUpdateTest(testData.filter, coll, testData.expected, testData);
    }
});

const illegalTests = [
    {
        // Cannot reference an encrypted field in a pipeline update.
        run: () => coll.updateOne(
            {}, [{$set: {"updateField": {$cond: [{$eq: ["$ssn", "123"]}, "123", "not123"]}}}])
    },
];

client.runEncryptionOperation(() => {
    for (const testData of illegalTests) {
        let failed = false;
        let res;
        try {
            res = testData.run();
        } catch (e) {
            res = tojson(e);
            failed = true;
        }

        assert(failed, {testData, commandRes: res});
    }
});
