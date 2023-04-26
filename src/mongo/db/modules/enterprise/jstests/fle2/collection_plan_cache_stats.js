/**
 * Verify plans are filtered through $planCacheStats if they are a QE collection or QE state
 * collection.
 *
 * @tags: [
 * # $planCacheStats can't run with a readConcern other than 'local'.
 * assumes_read_concern_unchanged,
 * # Reading from capped collections with readConcern snapshot is not supported.
 * requires_capped,
 * assumes_unsharded_collection,
 * requires_fcv_70,
 * # Bonsai optimizer cannot use the plan cache yet.
 * cqf_incompatible,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

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

assert.commandWorked(edb.basic.createIndex({a: 1}));
assert.commandWorked(edb.basic.createIndex({b: 1}));

assert.eq(0, dbTest.basic.aggregate([{$planCacheStats: {}}]).itcount());

assert.commandWorked(edb.basic.insert({_id: 0, "first": "mark", a: 1, b: 1}));
assert.commandWorked(edb.basic.insert({_id: 1, "first": "mark", a: 1, b: 1, c: 1}));
assert.commandWorked(edb.basic.insert({_id: 2, "first": "mark", a: 1, b: 1, c: 1, d: 1}));

// Run three distinct query shapes and check that there are 3 cache entries since this is not run in
// an encrypted client.
assert.eq(3, dbTest.basic.find({a: 1, b: 1}).itcount());
assert.eq(2, dbTest.basic.find({a: 1, b: 1, c: 1}).itcount());
assert.eq(1, dbTest.basic.find({a: 1, b: 1, d: 1}).itcount());
assert.eq(3, dbTest.basic.aggregate([{$planCacheStats: {}}]).itcount());
assert.commandWorked(dbTest.runCommand({planCacheClear: "basic"}));

// Run three distinct query shapes on the encrypted client and verify planCacheStats returns 0. This
// is because even though the query does not use encryption, the shell appends
// "encryptionInformation" which in turn filters the plans.
assert.eq(3, edb.basic.find({a: 1, b: 1}).itcount());
assert.eq(2, edb.basic.find({a: 1, b: 1, c: 1}).itcount());
assert.eq(1, edb.basic.find({a: 1, b: 1, d: 1}).itcount());
assert.eq(0, dbTest.basic.aggregate([{$planCacheStats: {}}]).itcount());
assert.commandWorked(dbTest.runCommand({planCacheClear: "basic"}));

// Run three distinct query shapes using also an encrypted field on the encrypted client and verify
// planCacheStats returns 0.
assert.eq(3, edb.basic.find({"first": "mark", a: 1, b: 1}).itcount());
assert.eq(2, edb.basic.find({"first": "mark", a: 1, b: 1, c: 1}).itcount());
assert.eq(1, edb.basic.find({"first": "mark", a: 1, b: 1, d: 1}).itcount());
assert.eq(0, dbTest.basic.aggregate([{$planCacheStats: {}}]).itcount());
}());
