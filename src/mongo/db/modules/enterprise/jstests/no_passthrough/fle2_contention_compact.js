/**
 * Test FLE2 compact works race under contention
 *
 * @tags: [
 *  requires_fcv_60
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");
load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallel_shell_helpers.js");
load("jstests/libs/uuid_util.js");

(function() {
'use strict';

const dbName = 'txn_contention_compact';
const collName = "basic";
const sampleEncryptedFields = {
    "fields": [
        {"path": "first", "bsonType": "string", "queries": {"queryType": "equality", contention: 0}}
    ]
};

const bgCompactFunc = function() {
    load("jstests/fle2/libs/encrypted_client_util.js");
    const client = new EncryptedClient(db.getMongo(), "txn_contention_compact");
    assert.commandWorked(client.getDB().basic.compact());
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

function runTest(conn, primaryConn) {
    const testDb = conn.getDB(dbName);
    const admin = primaryConn.getDB("admin");
    const isMongos = conn.isMongos();

    assert.commandWorked(testDb.setLogLevel(5, "sharding"));
    assert.commandWorked(primaryConn.getDB(dbName).setLogLevel(1, "sharding"));

    jsTestLog("Testing two simultaneous compacts are serialized");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const coll = edb[collName];
        const failpoint1 = "fleCompactHangBeforeESCAnchorInsert";
        const failpoint2 = "fleCompactFailBeforeECOCRead";

        // Setup a failpoint that hangs before ESC anchor insertion
        const fp1 = configureFailPoint(primaryConn, failpoint1);

        // Start the first compact, which hangs
        const bgCompactOne = startParallelShell(bgCompactFunc, conn.port);

        // Wait until the compact hits the failpoint
        fp1.wait();

        // Enable the failpoint that throws on subsequent compacts
        const fp2 = configureFailPoint(primaryConn, failpoint2);

        // Start the second compact which should not hit the throwing failpoint
        const bgCompactTwo = startParallelShell(bgCompactFunc, conn.port);

        // Not reliable, but need to delay so bgCompactTwo has a chance to actually send
        // the compact command, before the hanging failpoint is disabled.
        sleep(10 * 1000);

        // Disable the throwing failpoint
        fp2.off();

        // Unblock the first compact
        fp1.off();

        bgCompactOne();
        bgCompactTwo();

        // Only the first compact adds 1 anchor & removes non-anchors.
        // The second compact is a no-op since the first compact has emptied the ECOC.
        client.assertEncryptedCollectionCounts(collName, 10, 1, 0);
    });

    jsTestLog("Testing ECOC create when it already exists does not send back an error response");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        assert.commandWorked(testDb.setLogLevel(1, "storage"));

        const coll = edb[collName];
        const failpoint1 =
            isMongos ? "fleCompactHangBeforeECOCCreate" : "fleCompactHangBeforeECOCCreateUnsharded";

        // Setup a failpoint that hangs after ECOC rename, but before ECOC creation
        const fp = configureFailPoint(primaryConn, failpoint1);

        // Start the first compact, which hangs
        const bgCompactOne = startParallelShell(bgCompactFunc, conn.port);
        fp.wait();

        client.assertStateCollectionsAfterCompact(
            collName, false /* ecocExists */, true /* ecocTmpExists */);
        assert.commandWorked(coll.insert({_id: 11, "first": "mark"}));
        client.assertStateCollectionsAfterCompact(collName, true, true);

        // Unblock the first compact
        assert.commandWorked(admin.runCommand({configureFailPoint: failpoint1, mode: 'off'}));
        checkLog.containsJson(primaryConn, isMongos ? 7299603 : 7299602);

        bgCompactOne();
        client.assertStateCollectionsAfterCompact(collName, true, false);
        client.assertEncryptedCollectionCounts(collName, 11, 2, 1);
    });
}

jsTestLog("ReplicaSet: Testing fle2 contention on compact");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary(), rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 contention on compact");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});
    runTest(st.s, st.shard0);
    st.stop();
}
}());
