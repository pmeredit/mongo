/**
 * Verify top command omits information for QE collections and QE state collections.
 *
 * @tags: [
 * requires_profiling,
 * requires_fle2_in_always,
 * requires_fle2_encrypted_collscan,
 * requires_capped,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

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

    // TODO: SERVER-73303 remove once v2 is enabled by default
    if (!isFLE2ProtocolVersion2Enabled()) {
        assert(res.totals.hasOwnProperty("collection_top_stats.enxcol_.basic.ecc"));
        assert.eq(Object.keys(res.totals["collection_top_stats.enxcol_.basic.ecc"]).length, 0);
    }
}

jsTest.log("createEncryptionCollection");
assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

// Test $top command
let res = edb.adminCommand("top");
assert(res.hasOwnProperty("totals"));

// User QE collection will still have stats
assert(res.totals.hasOwnProperty("collection_top_stats.basic"));
assert.neq(Object.keys(res.totals["collection_top_stats.basic"]).length, 0);

assert(res.totals.hasOwnProperty("collection_top_stats.enxcol_.basic.ecoc"));
assert.eq(Object.keys(res.totals["collection_top_stats.enxcol_.basic.ecoc"]).length, 0);

assert(res.totals.hasOwnProperty("collection_top_stats.enxcol_.basic.esc"));
assert.eq(Object.keys(res.totals["collection_top_stats.enxcol_.basic.esc"]).length, 0);

// TODO: SERVER-73303 remove once v2 is enabled by default
if (!isFLE2ProtocolVersion2Enabled()) {
    assert(res.totals.hasOwnProperty("collection_top_stats.enxcol_.basic.ecc"));
    assert.eq(Object.keys(res.totals["collection_top_stats.enxcol_.basic.ecc"]).length, 0);
}

// Test insert command does not generate stats
assert.commandWorked(
    edb.basic.insert({"_id": 1, "first": "mark", "last": "marco", "middle": "markus"}));
res = edb.adminCommand("top");
assertNoEntries(res);

// Test udpate command does not generate stats
assert.commandWorked(edb.basic.update({"last": "marco"}, {$set: {"first": "matthew"}}));
res = edb.adminCommand("top");
assertNoEntries(res);

// Test findAndModify command does not generate stats
assert.commandWorked(edb.basic.runCommand(
    {findAndModify: edb.basic.getName(), query: {"last": "marco"}, remove: true}));
res = edb.adminCommand("top");
assertNoEntries(res);

// Test find command does not generate stats
assert.commandWorked(edb.basic.runCommand({find: "mark"}));
res = edb.adminCommand("top");
assertNoEntries(res);

// Test count command does not generate stats
assert.commandWorked(edb.runCommand({count: "basic"}));
res = edb.adminCommand("top");
assertNoEntries(res);

// Test delete command does not generate stats
assert.commandWorked(edb.basic.deleteOne({first: "mark"}));
res = edb.adminCommand("top");
assertNoEntries(res);

// Test aggregate command does not generate stats
edb.basic.aggregate([{$match: {first: "mark"}}]);
res = edb.adminCommand("top");
assertNoEntries(res);

// Test compactStructuredEncryptionData command does not generate stats
assert.commandFailedWithCode(
    edb.runCommand({"compactStructuredEncryptionData": "basic", compactionTokens: {}}), 6346806);
res = edb.adminCommand("top");
assertNoEntries(res);
}());
