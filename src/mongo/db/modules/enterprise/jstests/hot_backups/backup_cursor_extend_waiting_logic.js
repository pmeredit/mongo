/**
 * Test the waiting logic of $backupCursorExtend. Given a timestamp T, when
 * $backupCursorExtend returns, oplog with T should be majority committed and
 * persisent on the disk of that node.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_sharding,
 *   requires_wiredtiger,
 * ]
 */
import {
    copyBackupCursorExtendFiles,
    copyBackupCursorFiles,
    extendBackupCursor,
    getBackupCursorDB,
    openBackupCursor,
} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";
import {restartServerReplication, stopServerReplication} from "jstests/libs/write_concern_util.js";

const DEBUG = false;
const dbName = "test";
const collName = "coll";
const restorePath = MongoRunner.dataPath + "forRestore/";
const numDocs = 3000;

function insertDoc(db, collName, doc) {
    let res = assert.commandWorked(db.runCommand({insert: collName, documents: [doc]}));
    assert(res.hasOwnProperty("operationTime"), tojson(res));
    return res.operationTime;
}

function verifyData(backupPointInTime, numContinuousDocs) {
    let conn = MongoRunner.runMongod({
        dbpath: restorePath,
        noCleanData: true,
        setParameter: {recoverFromOplogAsStandalone: true}
    });
    assert.neq(conn, null);

    // Verify the top of the oplog
    let local = conn.getDB("local");
    let oplogTop =
        local.getCollection("oplog.rs").find().sort({$natural: -1}).limit(-1).next()["ts"];
    jsTestLog("Top of oplog: " + tojson(oplogTop) +
              ", backupPointInTime: " + tojson(backupPointInTime));
    assert(timestampCmp(oplogTop, backupPointInTime) >= 0);

    // Verify the existence of the continuous docs.
    let coll = conn.getDB(dbName).getCollection(collName);
    let docs = coll.find().sort({a: 1}).toArray();
    let total = docs.length;
    jsTestLog("There are " + total + " documents in total.");
    assert(total == numContinuousDocs);

    let docSet = new Set();
    for (let i = 0; i < total; i++) {
        docSet.add(docs[i].a);
    }
    for (let i = 0; i < total; i++) {
        assert(docSet.has(i), () => { return "Doc {a: " + i + "} is missing."; });
    }

    MongoRunner.stopMongod(conn, {noCleanData: true});
}

/*
 * Assert that lagged secondary will block when Timestamp T has not been majority committed yet.
 */
function assertLaggedSecondaryGetBlocked() {
    resetDbpath(restorePath);
    let rst = new ReplSetTest({name: "backupCursorExtendWaitingLogic", nodes: 3});
    rst.startSet();
    rst.initiate();
    const primaryDB = rst.getPrimary().getDB(dbName);
    const secondaryDB = rst.getSecondary().getDB(dbName);

    // The default WC is majority and this test can't satisfy majority writes.
    assert.commandWorked(rst.getPrimary().adminCommand(
        {setDefaultRWConcern: 1, defaultWriteConcern: {w: 1}, writeConcern: {w: "majority"}}));
    rst.awaitReplication();

    let cursor = openBackupCursor(rst.getSecondary().getDB("admin"));
    let firstBatch = cursor.next();
    assert("checkpointTimestamp" in firstBatch.metadata);
    const backupId = firstBatch.metadata.backupId;
    copyBackupCursorFiles(
        cursor, /*namespacesToSkip=*/[], firstBatch.metadata.dbpath, restorePath, false);

    // Stop advancing committedSnapshot timestamp on the secondary.
    stopServerReplication(rst.getSecondary());

    let clusterTime;
    jsTestLog("Start writes on primary");
    for (let i = 0; i < numDocs - 1; i++) {
        clusterTime = insertDoc(primaryDB, collName, {a: i});
    }
    jsTestLog("Finish " + numDocs + " insertions on primary");

    // Time out waiting for the clusterTime to be persistent on the secondary.
    assert.commandFailedWithCode(getBackupCursorDB(rst.getSecondary()).runCommand({
        aggregate: 1,
        pipeline: [{$backupCursorExtend: {backupId: backupId, timestamp: clusterTime}}],
        cursor: {},
        maxTimeMS: 20 * 1000
    }),
                                 ErrorCodes.MaxTimeMSExpired);

    restartServerReplication(rst.getSecondary());

    // Do another write in order to update the committedSnapshot value.
    clusterTime = insertDoc(primaryDB, collName, {a: numDocs - 1});

    jsTestLog("Extend backup cursor to " + tojson(clusterTime) + " on secondary");
    let now = (new Date()).getTime();
    let extendCursor = extendBackupCursor(rst.getSecondary(), backupId, clusterTime);
    let timeElapsed = (new Date()).getTime() - now;
    jsTestLog("Extend took " + timeElapsed + "ms");
    copyBackupCursorExtendFiles(
        extendCursor, /*namespacesToSkip=*/[], firstBatch.metadata.dbpath, restorePath, false);

    verifyData(clusterTime, numDocs);

    cursor.close();
    rst.stopSet();
}

/*
 * Assert that lagged shard will not block forever when noop writer is on.
 */
function assertLaggedShardCatchUpWithNoopWrites() {
    resetDbpath(restorePath);
    // We need noop writer to move forward the opTime on the lagged shard.
    const s = new ShardingTest({
        shards: 2,
        rs: {
            nodes: 1,
            syncdelay: 1,
            setParameter: {periodicNoopIntervalSecs: 1, writePeriodicNoops: true}
        }
    });
    const db = s.getDB(dbName);
    insertDoc(db, collName, {a: 0});
    const shardA = s.getPrimaryShard(dbName);
    const shardB = s.getOther(shardA);

    let cursor = openBackupCursor(shardB.getDB("admin"));
    let firstBatch = cursor.next();
    assert("checkpointTimestamp" in firstBatch.metadata);
    const backupId = firstBatch.metadata.backupId;
    copyBackupCursorFiles(
        cursor, /*namespacesToSkip=*/[], firstBatch.metadata.dbpath, restorePath, false);

    // Advance the cluster time by doing writes to shard A.
    jsTestLog("Start writes on shard A");
    let clusterTime;
    for (let i = 1; i < numDocs; i++) {
        clusterTime = insertDoc(db, collName, {a: i});
    }
    jsTestLog("Finish " + numDocs + " insertions on shard A");

    // Since all writes go to shard A, shard B does not have a valid opTime equal to or greater
    // than `clusterTime` until the noop writer moves the opTime forward.
    jsTestLog("Extend backup cursor to " + tojson(clusterTime) + " on shard B");
    let now = (new Date()).getTime();
    let extendCursor = extendBackupCursor(shardB, backupId, clusterTime);
    let timeElapsed = (new Date()).getTime() - now;
    jsTestLog("Extend took " + timeElapsed + "ms");
    copyBackupCursorExtendFiles(
        extendCursor, /*namespacesToSkip=*/[], firstBatch.metadata.dbpath, restorePath, false);

    verifyData(clusterTime, 0);

    if (DEBUG) {
        jsTestLog("shard A:" + tojson(shardA.getDB("local").oplog.rs.find().toArray()));
        jsTestLog("shard B:" + tojson(shardB.getDB("local").oplog.rs.find().toArray()));
    }

    cursor.close();
    s.stop();
}

assertLaggedSecondaryGetBlocked();
assertLaggedShardCatchUpWithNoopWrites();