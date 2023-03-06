/**
 * Verify collection coll stats gets filtered if it's a QE collection or a QE state collection
 *
 * @tags: [
 * requires_capped
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

// TODO: SERVER-73303 remove once v2 is enabled by default
if (isFLE2ProtocolVersion2Enabled()) {
    delete stateCollNss.ecc;
}
const qeNamespaces = ["basic", ...(Object.values(stateCollNss))];

// Test $collStats
for (let ns of qeNamespaces) {
    const errmsg = "Failed on namespace: " + ns;

    const res =
        edb[ns]
            .aggregate([{$collStats: {storageStats: {}, queryExecStats: {}, latencyStats: {}}}])
            .next();
    assert(res.hasOwnProperty("storageStats"), errmsg);
    assert(!res.storageStats.hasOwnProperty("wiredTiger"));
    assert(!res.hasOwnProperty("queryExecStats"));
    assert(!res.hasOwnProperty("latencyStats"));
}

// Test collStats
for (let ns of qeNamespaces) {
    const res = edb[ns].runCommand({collStats: "basic"});
    assert(!res.hasOwnProperty("wiredTiger"));
    assert(!res.hasOwnProperty("queryExecStats"));
    assert(!res.hasOwnProperty("latencyStats"));
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
        stat.ns == "collection_coll_stats.enxcol_.basic.ecc" ||
        stat.ns == "collection_coll_stats.enxcol_.basic.ecoc") {
        assert(stat.hasOwnProperty("storageStats"));
        assert(!stat.storageStats.hasOwnProperty("wiredTiger"));
        assert(!res.hasOwnProperty("queryExecStats"));
        assert(!res.hasOwnProperty("latencyStats"));
    }
}
}());
