/**
 * Verify plans are filtered through $planCacheStats if they are a QE collection or QE state
 * collection.
 *
 * @tags: [
 * # $planCacheStats can't run with a readConcern other than 'local'.
 * assumes_read_concern_unchanged,
 * # Reading from capped collections with readConcern snapshot is not supported.
 * requires_capped,
 * # Plan cache state is node-local and will not get migrated alongside user data
 * assumes_balancer_off,
 * assumes_unsharded_collection,
 * requires_fcv_70,
 * # Test expects to only talk to primary to verify plan cache usage
 * assumes_read_preference_unchanged,
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {getPlanCacheKeyFromShape} from "jstests/libs/query/analyze_plan.js";

// Asserts the number of expected documents and cached plans are found.
function assertDocumentAndPlanEntryCount(
    query, coll, expectedDocumentCount, expectedPlanCacheEntryCount) {
    assert.eq(expectedDocumentCount, coll.find(query).itcount());

    const keyHash = getPlanCacheKeyFromShape({query: query, collection: coll, db: db});
    const res = dbTest.basic.aggregate([{$planCacheStats: {}}, {$match: {planCacheKey: keyHash}}])
                    .toArray();

    assert.eq(expectedPlanCacheEntryCount, res.length);
}

function assertEncryptedDocumentAndPlanEntryCount(
    query, coll, expectedDocumentCount, expectedPlanCacheEntryCount) {
    assert.eq(expectedDocumentCount, coll.ecount(query));

    const keyHash = getPlanCacheKeyFromShape({query: query, collection: coll, db: db});
    const res = dbTest.basic.aggregate([{$planCacheStats: {}}, {$match: {planCacheKey: keyHash}}])
                    .toArray();

    assert.eq(expectedPlanCacheEntryCount, res.length);
}

const dbName = 'collection_plan_cache_stats';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);

const edb = client.getDB();

jsTest.log("createEncryptionCollection");
assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

// Clear plan cache and wait to prevent race conditions.
assert.commandWorked(dbTest.runCommand({planCacheClear: "basic"}));
sleep(1500);

assert.commandWorked(edb.basic.createIndex({a: 1}));
assert.commandWorked(edb.basic.createIndex({b: 1}));

assert.eq(0, dbTest.basic.aggregate([{$planCacheStats: {}}]).itcount());

assert.commandWorked(edb.basic.einsert({_id: 0, "first": "mark", a: 1, b: 1}));
assert.commandWorked(edb.basic.einsert({_id: 1, "first": "mark", a: 1, b: 1, c: 1}));
assert.commandWorked(edb.basic.einsert({_id: 2, "first": "mark", a: 1, b: 1, c: 1, d: 1}));

// Clear plan cache and wait to prevent race conditions.
assert.commandWorked(dbTest.runCommand({planCacheClear: "basic"}));
sleep(1500);

// Run three distinct query shapes and check that there is one cache entry for each find command
// since this is not run in an encrypted client.
assertDocumentAndPlanEntryCount({a: 1, b: 1}, dbTest.basic, 3, 1);
assertDocumentAndPlanEntryCount({a: 1, b: 1, c: 1}, dbTest.basic, 2, 1);
assertDocumentAndPlanEntryCount({a: 1, b: 1, d: 1}, dbTest.basic, 1, 1);

// Clear plan cache and wait to prevent race conditions.
assert.commandWorked(dbTest.runCommand({planCacheClear: "basic"}));
sleep(1500);

// Run three distinct query shapes on the encrypted client and verify planCacheStats returns 0. This
// is because even though the query does not use encryption, the shell appends
// "encryptionInformation" which in turn filters the plans.
assertEncryptedDocumentAndPlanEntryCount({a: 1, b: 1}, edb.basic, 3, 0);
assertEncryptedDocumentAndPlanEntryCount({a: 1, b: 1, c: 1}, edb.basic, 2, 0);
assertEncryptedDocumentAndPlanEntryCount({a: 1, b: 1, d: 1}, edb.basic, 1, 0);

// Clear plan cache and wait to prevent race conditions.
assert.commandWorked(dbTest.runCommand({planCacheClear: "basic"}));
sleep(1500);

// Run three distinct query shapes using also an encrypted field on the encrypted client and verify
// planCacheStats returns 0. We can not run getPlanCacheKeyFromShape with the encrypted field so we
// check the whole collction for no cached plans.
assert.eq(3, edb.basic.ecount({"first": "mark", a: 1, b: 1}));
assert.eq(2, edb.basic.ecount({"first": "mark", a: 1, b: 1, c: 1}));
assert.eq(1, edb.basic.ecount({"first": "mark", a: 1, b: 1, d: 1}));
assert.eq(0, dbTest.basic.aggregate([{$planCacheStats: {}}]).itcount());
