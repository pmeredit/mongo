/**
 * Test compact encrypted collection aggregate stats
 *
 * @tags: [
 *  requires_fcv_60,
 * ]
 */
import {isMongos} from "jstests/concurrency/fsm_workload_helpers/server_types.js";
import {runEncryptedTest} from "jstests/fle2/libs/encrypted_client_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

function insertInitialTestData(client, coll) {
    // Populate the EDC with sample data (6 unique values for "first")
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
    client.assertEncryptedCollectionCounts(coll.getName(), 32, 32, 32);

    const serverStats = client.getDB("admin").serverStatus();

    // Verify that insertion creates and increments emuBinaryStats.
    // (in v2, mongos no longer shows the emuBinaryStats since the emuBinary is done on mongod)
    if (!isMongos(client.getDB())) {
        const emuBinaryStats = serverStats.fle.emuBinaryStats;
        assert.gt(emuBinaryStats.calls, 0);
        assert.gt(emuBinaryStats.suboperations, 0);
    }

    // Verify that insertion creates and commits internal transactions
    const transactionStats = serverStats.internalTransactions;
    assert.gt(transactionStats.started, 0);
    assert.gt(transactionStats.succeeded, 0);
}

function runTest(conn, primaryConn) {
    const dbName = 'compact_collection_db';
    const collName = 'compact_stats';
    let db = conn.getDB(dbName);

    const sampleEncryptedFields = {
        fields: [
            {path: "first", bsonType: "string", queries: {"queryType": "equality", contention: 0}},
            {path: "ssn", bsonType: "string", queries: {"queryType": "equality", contention: 0}},
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
        // each ecoc entry is read once
        assert.eq(stats1.ecoc.read, NumberLong(32));
        assert.eq(stats1.ecoc.deleted, NumberLong(0));
        // (1 anchor insert) * 6
        assert.eq(stats1.esc.inserted, NumberLong(6));
        assert.eq(stats1.esc.updated, NumberLong(0));
        // 32 non-anchors are deleted
        assert.eq(stats1.esc.deleted, NumberLong(32));

        // The read count varies based on the exact way tags are generated because the keys are
        // non-deterministic, but a lower bound can be calculated as follows:
        // lower bound = 32 non-anchors read into delete set +
        //               6 * (4 AnchorBinaryHops reads) +
        //               {sum(1 + ceil(log2(32 + i)) for i in [0..5]}
        assert.gte(stats1.esc.read, NumberLong(97));

        client.assertEncryptedCollectionCounts(collName, 32, 6, 0);
        client.assertStateCollectionsAfterCompact(collName, true /* ecocExists */);

        let serverStatusFle = primaryConn.getDB("admin").serverStatus().fle.compactStats;
        assert.eq(serverStatusFle.ecoc.read, NumberLong(32));
        assert.eq(serverStatusFle.ecoc.deleted, NumberLong(0));
        assert.eq(serverStatusFle.esc.inserted, NumberLong(6));
        assert.eq(serverStatusFle.esc.updated, NumberLong(0));
        assert.eq(serverStatusFle.esc.deleted, NumberLong(32));
        assert.gte(serverStatusFle.esc.read, NumberLong(97));

        // Insert more non-unique values for "first"
        for (let i = 1; i <= 5; i++) {
            assert.commandWorked(coll.insert({"first": "roger"}));
            assert.commandWorked(coll.insert({"first": "roderick"}));
        }
        client.assertEncryptedCollectionCounts(collName, 42, 16, 10);

        // Compact the latest insertions, but now with an anchor present
        let stats2 = assert.commandWorked(coll.compact()).stats;
        print(tojson(stats2));
        assert.eq(stats2.ecoc.read, NumberLong(10));
        assert.eq(stats2.ecoc.deleted, NumberLong(0));
        // 2 anchors inserted
        assert.eq(stats2.esc.inserted, NumberLong(2));
        assert.eq(stats2.esc.updated, NumberLong(0));
        // 10 non-anchors were deleted
        assert.eq(stats2.esc.deleted, NumberLong(10));
        // lower bound ESC reads = 10 non-anchors read into delete set +
        //                         2 * (4 AnchorBinaryHops reads) +
        //                         (1 + ceil(log2(16))) + (1 + ceil(log2(17)))
        assert.gte(stats2.esc.read, NumberLong(29));

        client.assertEncryptedCollectionCounts(collName, 42, 8, 0);
        client.assertStateCollectionsAfterCompact(collName, true /* ecocExists */);

        serverStatusFle = primaryConn.getDB("admin").serverStatus().fle.compactStats;
        assert.eq(serverStatusFle.ecoc.read, NumberLong(42));
        assert.eq(serverStatusFle.ecoc.deleted, NumberLong(0));
        assert.eq(serverStatusFle.esc.inserted, NumberLong(8));
        assert.eq(serverStatusFle.esc.updated, NumberLong(0));
        assert.eq(serverStatusFle.esc.deleted, NumberLong(42));
        assert.gte(serverStatusFle.esc.read, NumberLong(126));
    });
}

jsTestLog("ReplicaSet: Testing fle2 compaction stats");
{
    const rst = new ReplSetTest({
        nodes: 1,
        nodeOptions: {setParameter: "unsupportedDangerousTestingFLEDiagnosticsEnabled=true"}
    });
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary(), rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 compaction stats");
{
    const st = new ShardingTest({
        shards: 1,
        mongos: 1,
        config: 1,
        mongosOptions: {
            setParameter: {unsupportedDangerousTestingFLEDiagnosticsEnabled: true},
        }
    });

    runTest(st.s, st.shard0);

    st.stop();
}
