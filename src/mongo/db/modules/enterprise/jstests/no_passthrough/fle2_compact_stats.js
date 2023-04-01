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
    client.assertEncryptedCollectionCounts(coll.getName(), 32, 32, 0, 32);

    const serverStats = client.getDB("admin").serverStatus();

    // Verify that insertion creates and increments emuBinaryStats.
    // (in v2, mongos no longer shows the emuBinaryStats since the emuBinary is done on mongod)
    // TODO: SERVER-73303 remove when v2 is enabled by default
    if (!isFLE2ProtocolVersion2Enabled() || !isMongos(client.getDB())) {
        const emuBinaryStats = serverStats.fle.emuBinaryStats;
        assert.gt(emuBinaryStats.calls, 0);
        assert.gt(emuBinaryStats.suboperations, 0);
    }

    // Verify that insertion creates and commits internal transactions
    const transactionStats = serverStats.internalTransactions;
    assert.gt(transactionStats.started, 0);
    assert.gt(transactionStats.succeeded, 0);
}

// TODO: SERVER-73303 remove when v2 is enabled by default
function runTestV1(conn, primaryConn) {
    if (isFLE2ProtocolVersion2Enabled()) {
        jsTest.log("Test skipped because featureFlagFLE2ProtocolVersion2 is enabled");
        return;
    }

    const dbName = 'compact_collection_db';
    const collName = 'compact_stats';
    const isMongos = conn.isMongos();
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
        // (1 null doc read attempt + 1 ipos read attempt) * 6
        assert.eq(stats1.ecc.read, NumberLong(12));
        assert.eq(stats1.ecc.inserted, NumberLong(0));
        assert.eq(stats1.ecc.updated, NumberLong(0));
        assert.eq(stats1.ecc.deleted, NumberLong(0));
        // (1 compaction placeholder insert + 1 null doc insert) * 6
        assert.eq(stats1.esc.inserted, NumberLong(12));
        assert.eq(stats1.esc.updated, NumberLong(0));
        // 32 entries deleted + 6 placeholders deleted
        assert.eq(stats1.esc.deleted, NumberLong(38));
        // the read count varies based on the exact way tags are generated because the keys are
        // non-deterministic
        assert.gte(stats1.esc.read, NumberLong(50));

        client.assertEncryptedCollectionCounts(collName, 32, 6, 0, 0);
        client.assertStateCollectionsAfterCompact(collName, true /* ecocExists */);

        let serverStatusFle = primaryConn.getDB("admin").serverStatus().fle.compactStats;
        assert.eq(serverStatusFle.ecoc.read, NumberLong(32));
        assert.eq(serverStatusFle.ecoc.deleted, NumberLong(0));
        assert.eq(serverStatusFle.ecc.read, NumberLong(12));
        assert.eq(serverStatusFle.ecc.inserted, NumberLong(0));
        assert.eq(serverStatusFle.ecc.updated, NumberLong(0));
        assert.eq(serverStatusFle.ecc.deleted, NumberLong(0));
        assert.eq(serverStatusFle.esc.inserted, NumberLong(12));
        assert.eq(serverStatusFle.esc.updated, NumberLong(0));
        assert.eq(serverStatusFle.esc.deleted, NumberLong(38));
        assert.gte(serverStatusFle.esc.read, NumberLong(50));

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
        client.assertStateCollectionsAfterCompact(collName, true /* ecocExists */);

        serverStatusFle = primaryConn.getDB("admin").serverStatus().fle.compactStats;
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

function runTest(conn, primaryConn) {
    // TODO: SERVER-73303 remove when v2 is enabled by default
    if (!isFLE2ProtocolVersion2Enabled()) {
        return;
    }

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
        assert(!stats1.hasOwnProperty("ecc"));

        // The read count varies based on the exact way tags are generated because the keys are
        // non-deterministic, but a lower bound can be calculated as follows:
        // lower bound = 32 non-anchors read into delete set +
        //               6 * (4 AnchorBinaryHops reads) +
        //               {sum(1 + ceil(log2(32 + i)) for i in [0..5]}
        assert.gte(stats1.esc.read, NumberLong(97));

        client.assertEncryptedCollectionCounts(collName, 32, 6, 0, 0);
        client.assertStateCollectionsAfterCompact(collName, true /* ecocExists */);

        let serverStatusFle = primaryConn.getDB("admin").serverStatus().fle.compactStats;
        assert.eq(serverStatusFle.ecoc.read, NumberLong(32));
        assert.eq(serverStatusFle.ecoc.deleted, NumberLong(0));
        assert.eq(serverStatusFle.esc.inserted, NumberLong(6));
        assert.eq(serverStatusFle.esc.updated, NumberLong(0));
        assert.eq(serverStatusFle.esc.deleted, NumberLong(32));
        assert.gte(serverStatusFle.esc.read, NumberLong(97));
        assert(!serverStatusFle.hasOwnProperty("ecc"));

        // Insert more non-unique values for "first"
        for (let i = 1; i <= 5; i++) {
            assert.commandWorked(coll.insert({"first": "roger"}));
            assert.commandWorked(coll.insert({"first": "roderick"}));
        }
        client.assertEncryptedCollectionCounts(collName, 42, 16, 0, 10);

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
        assert(!stats2.hasOwnProperty("ecc"));

        client.assertEncryptedCollectionCounts(collName, 42, 8, 0, 0);
        client.assertStateCollectionsAfterCompact(collName, true /* ecocExists */);

        serverStatusFle = primaryConn.getDB("admin").serverStatus().fle.compactStats;
        assert.eq(serverStatusFle.ecoc.read, NumberLong(42));
        assert.eq(serverStatusFle.ecoc.deleted, NumberLong(0));
        assert.eq(serverStatusFle.esc.inserted, NumberLong(8));
        assert.eq(serverStatusFle.esc.updated, NumberLong(0));
        assert.eq(serverStatusFle.esc.deleted, NumberLong(42));
        assert.gte(serverStatusFle.esc.read, NumberLong(126));
        assert(!serverStatusFle.hasOwnProperty("ecc"));
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
    runTestV1(rst.getPrimary(), rst.getPrimary());
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

    runTestV1(st.s, st.shard0);
    runTest(st.s, st.shard0);

    st.stop();
}
}());
