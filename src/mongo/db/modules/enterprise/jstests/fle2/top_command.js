/**
 * Verify top command omits information for QE collections and QE state collections.
 *
 * @tags: [
 * requires_profiling,
 * requires_fle2_in_always,
 * fle2_no_mongos,
 * requires_capped,
 * requires_fastcount,
 * requires_fcv_71,
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

const dbName = 'collection_top_stats';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);
const edb = client.getDB();

function assertNoEntries(res) {
    assert(res.hasOwnProperty("totals"));
    assert(res.totals.hasOwnProperty("collection_top_stats.basic"));
    assert.eq(Object.keys(res.totals["collection_top_stats.basic"]).length, 0);

    assert(res.totals.hasOwnProperty("collection_top_stats.enxcol_.basic.ecoc"));
    assert.eq(Object.keys(res.totals["collection_top_stats.enxcol_.basic.ecoc"]).length, 0);

    assert(res.totals.hasOwnProperty("collection_top_stats.enxcol_.basic.esc"));
    assert.eq(Object.keys(res.totals["collection_top_stats.enxcol_.basic.esc"]).length, 0);
}

jsTest.log("createEncryptionCollection");
assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

// Test $top command
let res = assert.commandWorked(edb.adminCommand("top"));
assert(res.hasOwnProperty("totals"));

// User QE collection will still have stats
assert(res.totals.hasOwnProperty("collection_top_stats.basic"));
assert.neq(Object.keys(res.totals["collection_top_stats.basic"]).length, 0);

assert(res.totals.hasOwnProperty("collection_top_stats.enxcol_.basic.ecoc"));
assert.eq(Object.keys(res.totals["collection_top_stats.enxcol_.basic.ecoc"]).length, 0);

assert(res.totals.hasOwnProperty("collection_top_stats.enxcol_.basic.esc"));
assert.eq(Object.keys(res.totals["collection_top_stats.enxcol_.basic.esc"]).length, 0);

// Test insert command does not generate stats
assert.commandWorked(
    edb.basic.einsert({"_id": 1, "first": "mark", "last": "marco", "middle": "markus"}));
res = assert.commandWorked(edb.adminCommand("top"));
assertNoEntries(res);

// Test udpate command does not generate stats
assert.commandWorked(edb.basic.eupdateOne({"last": "marco"}, {$set: {"first": "matthew"}}));
res = assert.commandWorked(edb.adminCommand("top"));
assertNoEntries(res);

// Test findAndModify command does not generate stats
assert.commandWorked(edb.basic.erunCommand(
    {findAndModify: edb.basic.getName(), query: {"last": "marco"}, remove: true}));
res = assert.commandWorked(edb.adminCommand("top"));
assertNoEntries(res);

// Test find command does not generate stats
assert.commandWorked(edb.basic.erunCommand({find: "mark"}));
res = assert.commandWorked(edb.adminCommand("top"));
assertNoEntries(res);

// Test count command does not generate stats
assert.commandWorked(edb.erunCommand({count: "basic"}));
res = assert.commandWorked(edb.adminCommand("top"));
assertNoEntries(res);

// Test delete command does not generate stats
assert.commandWorked(edb.basic.deleteOne({first: "mark"}));
res = assert.commandWorked(edb.adminCommand("top"));
assertNoEntries(res);

// Test aggregate command does not generate stats
edb.basic.aggregate([{$match: {first: "mark"}}]);
res = assert.commandWorked(edb.adminCommand("top"));
assertNoEntries(res);

// Test compactStructuredEncryptionData command does not generate stats
assert.commandFailedWithCode(
    edb.erunCommand({"compactStructuredEncryptionData": "basic", compactionTokens: {}}), 7294900);
res = assert.commandWorked(edb.adminCommand("top"));
assertNoEntries(res);

// Test cleanupStructuredEncryptionData command does not generate stats
client.runEncryptionOperation(() => { assert.commandWorked(edb.basic.cleanup()); });
res = assert.commandWorked(edb.adminCommand("top"));
assertNoEntries(res);
