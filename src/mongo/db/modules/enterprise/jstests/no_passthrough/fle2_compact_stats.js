/**
 * Test compact encrypted collection aggregate stats
 *
 * @tags: [
 *  requires_fcv_60,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

function insertInitialTestData(client, coll) {
    // Populate the EDC with sample data
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "roger", "alias": "rog", "ctr": i}));
        assert.commandWorked(coll.insert({"first": "roderick", "alias": "rod", "ctr": i}));
    }
    for (let i = 1; i <= 10; i++) {
        assert.commandWorked(coll.insert({"first": "ruben", "alias": "ben", "ctr": i}));
        assert.commandWorked(coll.insert({"first": "reginald", "alias": "reg", "ctr": i}));
    }
    assert.commandWorked(coll.insert({"first": "rudolf", "alias": "rudy", "ctr": 1}));
    assert.commandWorked(coll.insert({"first": "brian", "alias": "bri", "ctr": 1}));
    client.assertEncryptedCollectionCounts(coll.getName(), 32, 32, 0, 32);
}

function runTest(conn) {
    const dbName = 'compact_collection_db';
    const collName = 'compact_stats';
    let db = conn.getDB(dbName);

    const sampleEncryptedFields = {
        fields: [
            {path: "first", bsonType: "string", queries: {"queryType": "equality"}},
            {path: "ssn", bsonType: "string", queries: {"queryType": "equality"}},
        ]
    };

    // Verify server status has no fle section since we have no stats
    let serverStatus = db.serverStatus();
    assert(!serverStatus.hasOwnProperty("fle"));

    jsTestLog("Test normal compaction of inserts (ESC) only");
    runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
        const coll = edb[collName];
        insertInitialTestData(client, coll);

        // Compact each distinct value where no null doc is present yet
        let stats1 = assert.commandWorked(coll.compact()).stats;
        print(tojson(stats1));
        assert.eq(stats1.ecoc.read, NumberLong(32));
        assert.eq(stats1.ecoc.deleted, NumberLong(0));
        assert.eq(stats1.ecc.read, NumberLong(12));
        assert.eq(stats1.ecc.inserted, NumberLong(0));
        assert.eq(stats1.ecc.updated, NumberLong(0));
        assert.eq(stats1.ecc.deleted, NumberLong(0));
        assert.eq(stats1.esc.inserted, NumberLong(12));
        assert.eq(stats1.esc.updated, NumberLong(0));
        assert.eq(stats1.esc.deleted, NumberLong(38));
        // the read count varies based on the exact way tags are generated because the keys are
        // non-deterministic
        assert.gte(stats1.esc.read, NumberLong(51));

        client.assertEncryptedCollectionCounts(collName, 32, 6, 0, 0);
        client.assertStateCollectionsAfterCompact(collName);

        let serverStatusFle = db.serverStatus().fle.compactStats;
        assert.eq(serverStatusFle.ecoc.read, NumberLong(32));
        assert.eq(serverStatusFle.ecoc.deleted, NumberLong(0));
        assert.eq(serverStatusFle.ecc.read, NumberLong(12));
        assert.eq(serverStatusFle.ecc.inserted, NumberLong(0));
        assert.eq(serverStatusFle.ecc.updated, NumberLong(0));
        assert.eq(serverStatusFle.ecc.deleted, NumberLong(0));
        assert.eq(serverStatusFle.esc.inserted, NumberLong(12));
        assert.eq(serverStatusFle.esc.updated, NumberLong(0));
        assert.eq(serverStatusFle.esc.deleted, NumberLong(38));
        assert.gte(serverStatusFle.esc.read, NumberLong(51));

        // Insert more non-unique values for "first"
        for (let i = 1; i <= 5; i++) {
            assert.commandWorked(coll.insert({"first": "roger"}));
            assert.commandWorked(coll.insert({"first": "roderick"}));
        }
        client.assertEncryptedCollectionCounts(collName, 42, 16, 0, 10);

        // Compact the latest insertions, but now with null doc present
        let stats2 = assert.commandWorked(coll.compact());
        // Note read count for esc is more stable with a smaller number of documents
        assert.docEq(stats2.stats, {
            "ecoc": {"read": NumberLong(10), "deleted": NumberLong(0)},
            "ecc": {
                "read": NumberLong(4),
                "inserted": NumberLong(0),
                "updated": NumberLong(0),
                "deleted": NumberLong(0)
            },
            "esc": {
                "read": NumberLong(16),
                "inserted": NumberLong(2),
                "updated": NumberLong(2),
                "deleted": NumberLong(12)
            }
        });

        client.assertEncryptedCollectionCounts(collName, 42, 6, 0, 0);
        client.assertStateCollectionsAfterCompact(collName);

        serverStatusFle = db.serverStatus().fle.compactStats;
        assert.eq(serverStatusFle.ecoc.read, NumberLong(42));
        assert.eq(serverStatusFle.ecoc.deleted, NumberLong(0));
        assert.eq(serverStatusFle.ecc.read, NumberLong(16));
        assert.eq(serverStatusFle.ecc.inserted, NumberLong(0));
        assert.eq(serverStatusFle.ecc.updated, NumberLong(0));
        assert.eq(serverStatusFle.ecc.deleted, NumberLong(0));
        assert.eq(serverStatusFle.esc.inserted, NumberLong(14));
        assert.eq(serverStatusFle.esc.updated, NumberLong(2));
        assert.eq(serverStatusFle.esc.deleted, NumberLong(50));
        assert.gte(serverStatusFle.esc.read, NumberLong(60));
    });
}

jsTestLog("ReplicaSet: Testing fle2 compaction stats");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary());
    rst.stopSet();
}

// TODO SERVER-65170 - enable test
// jsTestLog("Sharding: Testing fle2 compaction stats");
// {
//     const st = new ShardingTest({shards: 1, mongos: 1, config: 1});

//     runTest(st.s);

//     st.stop();
// }
}());
