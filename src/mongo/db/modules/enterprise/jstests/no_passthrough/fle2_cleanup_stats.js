/**
 * Test QE cleanup command returns correct stats
 *
 * @tags: [
 * requires_fcv_71
 * ]
 */
import {isMongos} from "jstests/concurrency/fsm_workload_helpers/server_types.js";
import {runEncryptedTest} from "jstests/fle2/libs/encrypted_client_util.js";
import {
    QEStateCollectionStatsTracker
} from "jstests/fle2/libs/qe_state_collection_stats_tracker.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

function insertInitialTestData(client, coll, tracker) {
    let nEdc = 0;
    // Populate the EDC with sample data (6 unique values for "first")
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "roger", "alias": "rog", "ctr": i}));
        tracker.updateStatsPostInsert("first", "roger");
        nEdc++;

        assert.commandWorked(coll.insert({"first": "roderick", "alias": "rod", "ctr": i}));
        tracker.updateStatsPostInsert("first", "roderick");
        nEdc++;
    }
    for (let i = 1; i <= 10; i++) {
        assert.commandWorked(coll.insert({"first": "ruben", "alias": "ben", "ctr": i}));
        tracker.updateStatsPostInsert("first", "ruben");
        nEdc++;
        assert.commandWorked(coll.insert({"first": "reginald", "alias": "reg", "ctr": i}));
        tracker.updateStatsPostInsert("first", "reginald");
        nEdc++;
    }

    assert.commandWorked(coll.insert({"first": "rudolf", "alias": "rudy", "ctr": 1}));
    tracker.updateStatsPostInsert("first", "rudolf");
    nEdc++;
    assert.commandWorked(coll.insert({"first": "brian", "alias": "bri", "ctr": 1}));
    tracker.updateStatsPostInsert("first", "brian");
    nEdc++;

    let totals = tracker.calculateTotalStatsForFields("first");

    client.assertEncryptedCollectionCounts(coll.getName(), nEdc, totals.esc, totals.ecoc);
    client.assertESCNonAnchorCount(coll.getName(), totals.escNonAnchors);

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

    return nEdc;
}

function verifyCleanupStats(cleanupStats, trackerStats, readEstimate) {
    // each ecoc entry is read once
    assert.eq(cleanupStats.ecoc.read, NumberLong(trackerStats.ecoc));
    // cleanup never deletes from ECOC
    assert.eq(cleanupStats.ecoc.deleted, NumberLong(0));

    // # of ESC inserts is the # of values that got a new null anchor = 6
    assert.eq(cleanupStats.esc.inserted, NumberLong(trackerStats.escFutureNullAnchors));
    // # of ESC updates is the # of compacted values (6) minus those with new null anchors (6)
    assert.eq(cleanupStats.esc.updated,
              NumberLong(trackerStats.ecocUnique - trackerStats.escFutureNullAnchors));
    // # of ESC deletes is the total # of non-anchors (32) + deletable anchors (0) = 32
    assert.eq(cleanupStats.esc.deleted,
              NumberLong(trackerStats.escNonAnchors + trackerStats.escDeletableAnchors));
    // # of ESC reads is at least readEstimate
    assert.gte(cleanupStats.esc.read, NumberLong(readEstimate));
}

function verifyServerStatusCleanupStats(cleanupStats, previousServerStats, currentServerStats) {
    if (previousServerStats === undefined) {
        previousServerStats = {
            ecoc: {read: 0, deleted: 0},
            esc: {inserted: 0, deleted: 0, read: 0, updated: 0}
        };
    }
    assert.eq(currentServerStats.ecoc.read,
              NumberLong(previousServerStats.ecoc.read + cleanupStats.ecoc.read));
    assert.eq(currentServerStats.ecoc.deleted, NumberLong(0));

    assert.eq(currentServerStats.esc.inserted,
              NumberLong(previousServerStats.esc.inserted + cleanupStats.esc.inserted));
    assert.eq(currentServerStats.esc.updated,
              NumberLong(previousServerStats.esc.updated + cleanupStats.esc.updated));
    assert.eq(currentServerStats.esc.deleted,
              NumberLong(previousServerStats.esc.deleted + cleanupStats.esc.deleted));
    assert.eq(currentServerStats.esc.read,
              NumberLong(previousServerStats.esc.read + cleanupStats.esc.read));
}

function runTest(conn, primaryConn) {
    const dbName = 'cleanup_collection_db';
    const collName = 'cleanup_stats';
    let db = conn.getDB(dbName);

    const sampleEncryptedFields = {
        fields: [
            {path: "first", bsonType: "string", queries: {"queryType": "equality", contention: 0}},
        ]
    };

    // Verify server status has no fle section since we have no stats
    let serverStatus = db.serverStatus();
    assert(!serverStatus.hasOwnProperty("fle"));

    jsTestLog("Test normal cleanup of inserts");
    runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
        const coll = edb[collName];
        const tracker = new QEStateCollectionStatsTracker();

        let nEdc = insertInitialTestData(client, coll, tracker);

        // Get tracked stats after inserts & before cleanup
        let totals = tracker.calculateTotalStatsForFields("first");

        // Get the expected read stats estimate if we perform cleanup
        let readEstimate = tracker.calculateEstimatedESCReadCountForCleanup("first");

        // Cleanup each distinct value where no null anchor is present yet
        let stats1 = assert.commandWorked(coll.cleanup()).stats;

        let serverStats1 = primaryConn.getDB("admin").serverStatus().fle.cleanupStats;

        print("Stats from first cleanup: " + tojson(stats1));
        print("Tracked stats before first cleanup: " + tojson(totals));
        print("Estimated read count for first cleanup: " + readEstimate);
        print("Server status after first cleanup: " + tojson(serverStats1));

        verifyCleanupStats(stats1, totals, readEstimate);

        verifyServerStatusCleanupStats(stats1, undefined, serverStats1);

        tracker.updateStatsPostCleanupForFields("first");
        totals = tracker.calculateTotalStatsForFields("first");

        client.assertEncryptedCollectionCounts(collName, nEdc, totals.esc, totals.ecoc);
        client.assertESCNonAnchorCount(collName, totals.escNonAnchors);
        client.assertStateCollectionsAfterCompact(collName, true /* ecocExists */);

        // Insert more non-unique values for "first"
        for (let i = 1; i <= 5; i++) {
            assert.commandWorked(coll.insert({"first": "roger"}));
            tracker.updateStatsPostInsert("first", "roger");
            nEdc++;
            assert.commandWorked(coll.insert({"first": "roderick"}));
            tracker.updateStatsPostInsert("first", "roderick");
            nEdc++;
        }
        totals = tracker.calculateTotalStatsForFields("first");
        client.assertEncryptedCollectionCounts(collName, nEdc, totals.esc, totals.ecoc);
        client.assertESCNonAnchorCount(collName, totals.escNonAnchors);

        // Cleanup the latest insertions
        let stats2 = assert.commandWorked(coll.cleanup()).stats;
        let serverStats2 = primaryConn.getDB("admin").serverStatus().fle.cleanupStats;

        readEstimate = tracker.calculateEstimatedESCReadCountForCleanup("first");
        print("Stats from second cleanup: " + tojson(stats2));
        print("Tracked stats before second cleanup: " + tojson(totals));
        print("Estimated read count for second cleanup: " + readEstimate);
        print("Server status after second cleanup: " + tojson(serverStats2));

        verifyCleanupStats(stats2, totals, readEstimate);

        verifyServerStatusCleanupStats(stats2, serverStats1, serverStats2);

        tracker.updateStatsPostCleanupForFields("first");
        totals = tracker.calculateTotalStatsForFields("first");

        client.assertEncryptedCollectionCounts(collName, nEdc, totals.esc, totals.ecoc);
        client.assertESCNonAnchorCount(collName, totals.escNonAnchors);
        client.assertStateCollectionsAfterCompact(collName, true /* ecocExists */);
    });
}

jsTestLog("ReplicaSet: Testing fle2 cleanup stats");
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

jsTestLog("Sharding: Testing fle2 cleanup stats");
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
