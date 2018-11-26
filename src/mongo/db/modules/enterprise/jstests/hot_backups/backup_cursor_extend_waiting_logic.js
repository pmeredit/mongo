/**
 * Test the waiting logic of $backupCursorExtend. Given a timestamp T, when
 * $backupCursorExtend returns, oplog with T should be majority committed and
 * persisent on the disk of that node.
 *
 * @tags: [requires_sharding,
 *         requires_wiredtiger,
 *         requires_journaling,
 *         requires_persistence,
 *         requires_majority_read_concern]
 */
(function() {
    "use strict";

    const DEBUG = false;
    const dbName = "test";
    const collName = "coll";

    function openBackupCursor(db) {
        return db.aggregate([{$backupCursor: {}}]).next().metadata.backupId;
    }

    function extendBackupCursor(db, parameters, timeoutMS) {
        return db.runCommand({
            aggregate: 1,
            pipeline: [{$backupCursorExtend: parameters}],
            cursor: {},
            maxTimeMS: timeoutMS
        });
    }

    function insertDoc(db, collName, doc) {
        let res = assert.commandWorked(db.runCommand({insert: collName, documents: [doc]}));
        assert(res.hasOwnProperty("operationTime"), tojson(res));
        return res.operationTime;
    }

    /*
     * Assert that lagged secondary will block when Timestamp T has not been majority committed yet.
     */
    function assertLaggedSecondaryGetBlocked() {
        let rst = new ReplSetTest({name: "backupCursorExtendWaitingLogic", nodes: 3});
        rst.startSet();
        rst.initiate();
        const primaryDB = rst.getPrimary().getDB(dbName);
        const secondaryDB = rst.getSecondary().getDB(dbName);

        const backupId = openBackupCursor(secondaryDB);

        // Stop advancing committedSnapshot timestamp on the secondary.
        assert.commandWorked(rst.getSecondary().adminCommand(
            {configureFailPoint: "disableSnapshotting", mode: "alwaysOn"}));

        let clusterTime = insertDoc(primaryDB, collName, {});
        // Time out waiting for the clusterTime to be persistent on the secondary.
        assert.commandFailedWithCode(
            extendBackupCursor(secondaryDB, {backupId: backupId, timestamp: clusterTime}, 2000),
            ErrorCodes.MaxTimeMSExpired);

        assert.commandWorked(rst.getSecondary().adminCommand(
            {configureFailPoint: "disableSnapshotting", mode: "off"}));

        // Do another write in order to update the committedSnapshot value.
        clusterTime = insertDoc(primaryDB, collName, {});
        assert.commandWorked(
            extendBackupCursor(secondaryDB, {backupId: backupId, timestamp: clusterTime}, 2000));

        rst.stopSet();
    }

    /*
     * Assert that lagged shard will not block forever when noop writer is on.
     */
    function assertLaggedShardCatchUpWithNoopWrites() {
        // We need noop writer to move forward the opTime on the lagged shard.
        const s = new ShardingTest({
            shards: 2,
            rs: {nodes: 1, setParameter: {periodicNoopIntervalSecs: 1, writePeriodicNoops: true}}
        });
        const db = s.getDB(dbName);
        insertDoc(db, collName, {a: 1});
        const shardA = s.getPrimaryShard(dbName);
        const shardB = s.getOther(shardA);

        const backupId = openBackupCursor(shardB.getDB(dbName));

        // Advance the cluster time by doing writes to shard A.
        let clusterTime;
        for (let i = 2; i < 30; i++) {
            clusterTime = insertDoc(db, collName, {a: i});
        }

        // Since all writes go to shard A, shard B does not have a valid opTime equal to or greater
        // than `clusterTime` until the noop writer moves the opTime forward.
        assert.commandWorked(extendBackupCursor(
            shardB.getDB(dbName), {backupId: backupId, timestamp: clusterTime}, 2000));

        if (DEBUG) {
            jsTestLog("shard A:" + tojson(shardA.getDB("local").oplog.rs.find().toArray()));
            jsTestLog("shard B:" + tojson(shardB.getDB("local").oplog.rs.find().toArray()));
        }

        s.stop();
    }

    assertLaggedSecondaryGetBlocked();
    assertLaggedShardCatchUpWithNoopWrites();
})();
