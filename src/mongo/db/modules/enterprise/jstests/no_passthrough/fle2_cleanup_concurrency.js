/**
 * Test FLE2 cleanups are serialized with itself & compactions.
 *
 * @tags: [
 * requires_fcv_71
 * ]
 */

import {runEncryptedTest} from "jstests/fle2/libs/encrypted_client_util.js";
import {configureFailPoint} from "jstests/libs/fail_point_util.js";

const dbName = 'fle2_cleanup_concurrency';
const collName = "basic";
const sampleEncryptedFields = {
    "fields": [
        {"path": "first", "bsonType": "string", "queries": {"queryType": "equality", contention: 0}}
    ]
};

const bgCleanupFunc = async function() {
    const {EncryptedClient} = await import("jstests/fle2/libs/encrypted_client_util.js");
    const client = new EncryptedClient(db.getMongo(), "fle2_cleanup_concurrency");

    client.runEncryptionOperation(() => { assert.commandWorked(client.getDB().basic.cleanup()); });
};

const bgCompactFunc = async function() {
    const {EncryptedClient} = await import("jstests/fle2/libs/encrypted_client_util.js");
    const client = new EncryptedClient(db.getMongo(), "fle2_cleanup_concurrency");
    client.runEncryptionOperation(() => { assert.commandWorked(client.getDB().basic.compact()); });
};

function setupTest(client) {
    const coll = client.getDB()[collName];

    // Insert data to compact
    for (let i = 1; i <= 10; i++) {
        assert.commandWorked(coll.insert({_id: i, "first": "mark"}));
    }
    client.assertEncryptedCollectionCounts(collName, 10, 10, 10);
    return client;
}

function runSerializedOpsTest(
    conn, primaryConn, bgOperation1, bgOperation2, hangingFailpoint, throwingFailpoint) {
    // Set up the failpoint that causes bgOperation1 to hang
    const fp1 = configureFailPoint(primaryConn, hangingFailpoint);

    // Start the first op; wait until it hits the hanging failpoint
    const bgOpOne = startParallelShell(bgOperation1, conn.port);
    fp1.wait();

    // Set up the failpoint that would cause bgOperation2 to throw if it enters
    // the critical section.
    const fp2 = configureFailPoint(primaryConn, throwingFailpoint);

    // Start the second op; it should not reach the throwing failpoint
    const bgOpTwo = startParallelShell(bgOperation2, conn.port);

    // Delay so bgOpTwo has a chance to actually send its command, before failpoints are disabled.
    sleep(10 * 1000);

    // Disable failpoints; the throwing one first
    fp2.off();
    fp1.off();

    // Run parallel shells to completion
    bgOpOne();
    bgOpTwo();
}

function runTest(conn, primaryConn) {
    const testDb = conn.getDB(dbName);
    const admin = primaryConn.getDB("admin");
    const isMongos = conn.isMongos();

    assert.commandWorked(primaryConn.getDB(dbName).setLogLevel(1, "storage"));

    jsTestLog("Testing two simultaneous cleanups are serialized");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const fp1 = "fleCleanupHangBeforeNullAnchorUpdate";
        const fp2 = "fleCompactOrCleanupFailBeforeECOCRead";
        runSerializedOpsTest(conn, primaryConn, bgCleanupFunc, bgCleanupFunc, fp1, fp2);

        // The first cleanup adds 1 null anchor & removes non-anchors
        // The second cleanup is a no-op since the first cleanup has emptied the ECOC
        client.assertEncryptedCollectionCounts(collName, 10, 1, 0);
    });

    jsTestLog("Testing simultaneous cleanup and compact are serialized");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const fp1 = "fleCleanupHangBeforeNullAnchorUpdate";
        const fp2 = "fleCompactOrCleanupFailBeforeECOCRead";
        runSerializedOpsTest(conn, primaryConn, bgCleanupFunc, bgCompactFunc, fp1, fp2);

        // The first cleanup adds 1 null anchor & removes non-anchors
        // The succeeding compact is a no-op since the first cleanup has emptied the ECOC
        client.assertEncryptedCollectionCounts(collName, 10, 1, 0);
    });

    jsTestLog("Testing simultaneous compact and cleanup are serialized");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const fp1 = "fleCompactHangBeforeESCAnchorInsert";
        const fp2 = "fleCompactOrCleanupFailBeforeECOCRead";
        runSerializedOpsTest(conn, primaryConn, bgCompactFunc, bgCleanupFunc, fp1, fp2);

        // The first compact adds 1 anchor & removes non-anchors
        // The succeeding cleanup is a no-op since the first compact has emptied the ECOC
        client.assertEncryptedCollectionCounts(collName, 10, 1, 0);
    });
}

jsTestLog("ReplicaSet: Testing fle2 cleanup concurrency");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary(), rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 cleanup concurrency");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});

    st.forEachConnection(
        (conn) => { assert.commandWorked(conn.getDB(dbName).setLogLevel(1, "sharding")); });

    runTest(st.s, st.shard0);
    st.stop();
}
