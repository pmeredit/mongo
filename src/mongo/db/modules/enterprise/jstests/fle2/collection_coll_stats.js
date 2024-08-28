/**
 * Verify collection coll stats gets filtered if it's a QE collection or a QE state collection
 *
 * @tags: [
 * requires_capped,
 * requires_collstats,
 * requires_fcv_70,
 * requires_persistence,
 * requires_getmore,
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

function validateStats(stats, errmsg) {
    print("STATS: " + tojson(stats));
    assert(stats.hasOwnProperty("uri"), errmsg);
    assert.eq(Object.keys(stats).length, 1, errmsg);
}

function validateStorageStats(storageStats, errmsg) {
    print("S_STATS: " + tojson(storageStats));
    assert(storageStats.hasOwnProperty("wiredTiger", errmsg));
    validateStats(storageStats.wiredTiger, errmsg);

    for (const index in storageStats.indexDetails) {
        const details = storageStats.indexDetails[index];

        if (!details.hasOwnProperty("clustered")) {
            validateStats(details, errmsg);
        }
    }
}

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

    assert(!res.hasOwnProperty("queryExecStats", errmsg));
    assert(!res.hasOwnProperty("latencyStats", errmsg));

    assert(res.hasOwnProperty("storageStats"), errmsg);
    validateStorageStats(res.storageStats, errmsg);
}

// Test collStats
for (let ns of qeNamespaces) {
    const errmsg = "Failed on namespace: " + ns;

    const res = edb[ns].runCommand({collStats: "basic"});
    assert(res.hasOwnProperty("wiredTiger", errmsg));
    validateStats(res.wiredTiger);

    assert(!res.hasOwnProperty("queryExecStats", errmsg));
    assert(!res.hasOwnProperty("latencyStats", errmsg));

    for (const index in res.indexDetails) {
        const details = res.indexDetails[index];

        if (!details.hasOwnProperty("clustered")) {
            validateStats(details, errmsg);
        }
    }
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

        assert(stat.hasOwnProperty("storageStats", errmsg));
        validateStorageStats(stat.storageStats, errmsg);

        assert(!res.hasOwnProperty("queryExecStats", errmsg));
        assert(!res.hasOwnProperty("latencyStats", errmsg));
    }
}
