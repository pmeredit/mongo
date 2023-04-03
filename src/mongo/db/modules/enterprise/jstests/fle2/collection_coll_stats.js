/**
 * Verify collection coll stats gets filtered if it's a QE collection or a QE state collection
 *
 * @tags: [
 * requires_capped,
 * requires_collstats,
 * requires_fcv_70,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

const dbName = 'collection_coll_stats';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);

const edb = client.getDB();
const adminDB = client.getAdminDB();

jsTest.log("createEncryptionCollection");
assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

const stateCollNss = client.getStateCollectionNamespaces("basic");

const qeNamespaces = ["basic", ...(Object.values(stateCollNss))];

// Test $collStats
for (let ns of qeNamespaces) {
    const errmsg = "Failed on namespace: " + ns;

    const res =
        edb[ns]
            .aggregate([{$collStats: {storageStats: {}, queryExecStats: {}, latencyStats: {}}}])
            .next();
    assert(res.hasOwnProperty("storageStats"), errmsg);
    assert(!res.storageStats.hasOwnProperty("wiredTiger"), errmsg);
    assert(!res.hasOwnProperty("queryExecStats"), errmsg);
    assert(!res.hasOwnProperty("latencyStats"), errmsg);
}

// Test collStats
for (let ns of qeNamespaces) {
    const errmsg = "Failed on namespace: " + ns;

    const res = edb[ns].runCommand({collStats: "basic"});
    assert(!res.hasOwnProperty("wiredTiger"), errmsg);
    assert(!res.hasOwnProperty("queryExecStats"), errmsg);
    assert(!res.hasOwnProperty("latencyStats"), errmsg);
}

// Test $_internalAllCollectionStats
const res = adminDB
                .aggregate([{
                    $_internalAllCollectionStats:
                        {stats: {storageStats: {}, queryExecStats: {}, latencyStats: {}}}
                }])
                .toArray();
for (const stat of res) {
    // If a QE collection or a QE state collection
    if (stat.ns == "collection_coll_stats.basic" ||
        stat.ns == "collection_coll_stats.enxcol_.basic.esc" ||
        stat.ns == "collection_coll_stats.enxcol_.basic.ecoc") {
        const errmsg = "Failed on namespace: " + stat.ns;
        assert(stat.hasOwnProperty("storageStats"), errmsg);
        assert(!stat.storageStats.hasOwnProperty("wiredTiger"), errmsg);
        assert(!res.hasOwnProperty("queryExecStats"), errmsg);
        assert(!res.hasOwnProperty("latencyStats"), errmsg);
    }
}
}());
