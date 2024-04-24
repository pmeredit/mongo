/**
 * Test for explain of findAndModify over encrypted fields.
 *
 * @tags: [
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   assumes_unsharded_collection,
 *   assumes_write_concern_unchanged,
 *   requires_fcv_70,
 *   requires_fle2_in_always,
 * ]
 */
import {EncryptedClient, kSafeContentField} from "jstests/fle2/libs/encrypted_client_util.js";
import {getPlanStage} from "jstests/libs/analyze_plan.js";

const dbName = jsTestName();
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const collName = "test";

const client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection(collName, {
    encryptedFields: {
        "fields": [
            {"path": "secretString", "bsonType": "string", "queries": {"queryType": "equality"}},
            {"path": "nested.secretInt", "bsonType": "int", "queries": {"queryType": "equality"}}
        ]
    }
}));

const edb = client.getDB();
const coll = edb.getCollection(collName);
assert.commandWorked(
    coll.einsert({_id: 1, secretString: "1337", nested: {secretInt: NumberInt(1337)}}));
client.assertEncryptedCollectionCounts(coll.getName(), 1, 2, 2);

// Test that explain shows the rewritten access plan over __safeContent__ for the filter portion.
let explain = assert.commandWorked(edb.erunCommand({
    explain: {
        findAndModify: collName,
        query: {secretString: "1337"},
        update: {$set: {is1337: true}},
    },
    verbosity: "queryPlanner"
}));
let planStage = getPlanStage(explain, "IXSCAN");
assert.neq(null, planStage);
assert.eq(planStage.keyPattern, {[kSafeContentField]: 1}, tojson(planStage));

explain = assert.commandWorked(edb.erunCommand({
    explain: {
        findAndModify: collName,
        query: {'nested.secretInt': NumberInt(1337)},
        update: {$set: {found: true}},
    },
    verbosity: "queryPlanner"
}));
planStage = getPlanStage(explain, "IXSCAN");
assert.neq(null, planStage);
assert.eq(planStage.keyPattern, {[kSafeContentField]: 1}, tojson(planStage));

// Test that the update portion of the explain result shows the constant marked for encryption but
// does not actually perform any writes to neither the data collection nor the state collections.
explain = assert.commandWorked(edb.erunCommand({
    explain: {
        findAndModify: collName,
        query: {_id: 1},
        update: {$set: {secretString: "not1337"}},
    },
    verbosity: "queryPlanner"
}));

client.assertEncryptedCollectionDocuments(coll.getName(), [
    {_id: 1, secretString: "1337", nested: {secretInt: NumberInt(1337)}},
]);
client.assertEncryptedCollectionCounts(coll.getName(), 1, 2, 2);

// Similar test but unsetting an encrypted field.
explain = assert.commandWorked(edb.erunCommand({
    explain: {
        findAndModify: collName,
        query: {_id: 1},
        update: {$unset: {secretString: 1}},
    },
    verbosity: "queryPlanner"
}));
client.assertEncryptedCollectionCounts(coll.getName(), 1, 2, 2);

// Verify that explain with executionStats verbosity does not modify data or state collections.
explain = assert.commandWorked(edb.erunCommand({
    explain: {
        findAndModify: collName,
        query: {_id: 1},
        update: {$unset: {secretString: 1}},
    },
    verbosity: "executionStats"
}));
client.assertEncryptedCollectionDocuments(coll.getName(), [
    {_id: 1, secretString: "1337", nested: {secretInt: NumberInt(1337)}},
]);
client.assertEncryptedCollectionCounts(coll.getName(), 1, 2, 2);
