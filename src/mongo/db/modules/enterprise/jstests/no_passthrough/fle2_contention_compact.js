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
    client.assertEncryptedCollectionCounts(collName, 10, 10, 0, 10);
    return client;
}

// TODO: SERVER-73303 remove when v2 is enabled by default
function assertWriteConflict(conn, lsid = null) {
    const attrs = {
        error: {codeName: "WriteConflict"},
    };
    if (lsid) {
        attrs["txnInfo"] = {sessionInfo: {lsid: {id: {"$uuid": extractUUIDFromObject(lsid)}}}};
    }
    assert.soon(function() {
        return checkLog.checkContainsWithAtLeastCountJson(conn, 5875905, attrs, 1, null, true);
    }, "Expected write conflicts, but none were found.", 10 * 1000);
}

// TODO: SERVER-73303 remove when v2 is enabled by default
function runTestV1(conn, primaryConn) {
    if (isFLE2ProtocolVersion2Enabled()) {
        jsTest.log("Test skipped because featureFlagFLE2ProtocolVersion2 is enabled");
        return;
    }
    const testDb = conn.getDB(dbName);
    const admin = primaryConn.getDB("admin");
    const isMongos = conn.isMongos();
    const failpointWaitTimeoutMS = 5 * 60 * 1000;

    assert.commandWorked(testDb.setLogLevel(1, "sharding"));
    assert.commandWorked(primaryConn.getDB(dbName).setLogLevel(1, "sharding"));

    // Enable this failpoint so that write conflict errors are returned sooner
    assert.commandWorked(conn.adminCommand(
        {configureFailPoint: "overrideTransactionApiMaxRetriesToThree", mode: 'alwaysOn'}));

    jsTestLog("Testing ESC compact write conflict with a FLE2 insert retries the ESC compact");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const coll = edb[collName];
        const failpoint = "fleCompactHangBeforeESCPlaceholderInsert";

        // Setup a failpoint that hangs before ESC compaction placeholder insert
        const hitCount =
            assert
                .commandWorked(admin.runCommand({configureFailPoint: failpoint, mode: 'alwaysOn'}))
                .count;

        // Start a compact, which will hang before the ESC placeholder insert
        const bgCompact = startParallelShell(bgCompactFunc, conn.port);
        assert.commandWorked(admin.runCommand({
            waitForFailPoint: failpoint,
            timesEntered: hitCount + 1,
            maxTimeMS: failpointWaitTimeoutMS
        }));
        client.assertEncryptedCollectionCounts(collName, 10, 10, 0, 0);

        // Insert another entry in the next position: 11
        assert.commandWorked(coll.insert({_id: 11, "first": "mark"}));
        client.assertEncryptedCollectionCounts(collName, 11, 11, 0, 1);

        // Unblock the compact
        assert.commandWorked(admin.runCommand({configureFailPoint: failpoint, mode: 'off'}));
        bgCompact();

        // check log for write conflict error
        assertWriteConflict(primaryConn);
        client.assertEncryptedCollectionCounts(collName, 11, 1, 0, 1);
    });

    jsTestLog("Testing FLE2 insert write conflict with ESC compact retries the insert");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const coll = edb[collName];
        const failpoint = "fleCompactHangAfterESCPlaceholderInsert";

        // Set up a failpoint to hang after the ESC compaction placeholder insert
        const hitCount =
            assert
                .commandWorked(admin.runCommand({configureFailPoint: failpoint, mode: 'alwaysOn'}))
                .count;

        // Start a compact, which will hang after the ESC placeholder insert
        const bgCompact = startParallelShell(bgCompactFunc, conn.port);
        assert.commandWorked(admin.runCommand({
            waitForFailPoint: failpoint,
            timesEntered: hitCount + 1,
            maxTimeMS: failpointWaitTimeoutMS
        }));
        client.assertEncryptedCollectionCounts(collName, 10, 10, 0, 0);

        // Start an insert which will keep retrying and fail due to write conflict
        const res = coll.insert({_id: 11, "first": "mark"});
        assert.commandFailedWithCode(
            res, ErrorCodes.WriteConflict, "insert did not fail with the expected code");

        // Unblock the compact
        assert.commandWorked(admin.runCommand({configureFailPoint: failpoint, mode: 'off'}));
        bgCompact();
        client.assertEncryptedCollectionCounts(collName, 10, 1, 0, 0);
    });

    jsTestLog("Testing ECC compact write conflict with a FLE2 delete retries the ECC compact");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        assert.commandWorked(admin.runCommand({clearLog: "global"}));
        setupTest(client);

        const coll = edb[collName];
        const failpoint = "fleCompactHangBeforeECCPlaceholderInsert";

        for (let i = 1; i <= 5; i++) {
            assert.commandWorked(coll.deleteOne({_id: i}));
        }
        client.assertEncryptedCollectionCounts(collName, 5, 10, 5, 15);

        // Setup a failpoint that hangs before ECC compaction placeholder insert
        const hitCount =
            assert
                .commandWorked(admin.runCommand({configureFailPoint: failpoint, mode: 'alwaysOn'}))
                .count;

        // Start a compact, which will hang before the ECC placeholder insert at pos 6
        const bgCompact = startParallelShell(bgCompactFunc, conn.port);
        assert.commandWorked(admin.runCommand({
            waitForFailPoint: failpoint,
            timesEntered: hitCount + 1,
            maxTimeMS: failpointWaitTimeoutMS
        }));

        // ESC now has the placeholder at this point
        client.assertEncryptedCollectionCounts(collName, 5, 10, 5, 0);

        // Delete another entry, which inserts an ECC entry in the next position: 6
        assert.commandWorked(coll.deleteOne({_id: 6}));
        client.assertEncryptedCollectionCounts(collName, 4, 10, 6, 1);

        // Unblock the compact
        assert.commandWorked(admin.runCommand({configureFailPoint: failpoint, mode: 'off'}));
        bgCompact();

        // check log for write conflict error
        assertWriteConflict(primaryConn);
        client.assertEncryptedCollectionCounts(collName, 4, 1, 2, 1);
    });

    jsTestLog("Testing FLE2 delete write conflict with ECC compact retries the delete");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const coll = edb[collName];
        const failpoint = "fleCompactHangAfterECCPlaceholderInsert";

        for (let i = 1; i <= 5; i++) {
            assert.commandWorked(coll.deleteOne({_id: i}));
        }
        client.assertEncryptedCollectionCounts(collName, 5, 10, 5, 15);

        // Set up a failpoint to hang after the ECC compaction placeholder insert
        const hitCount =
            assert
                .commandWorked(admin.runCommand({configureFailPoint: failpoint, mode: 'alwaysOn'}))
                .count;

        // Start a compact, which will hang after the ECC placeholder insert
        const bgCompact = startParallelShell(bgCompactFunc, conn.port);
        assert.commandWorked(admin.runCommand({
            waitForFailPoint: failpoint,
            timesEntered: hitCount + 1,
            maxTimeMS: failpointWaitTimeoutMS
        }));
        // ESC and ECC now have placeholders at this point
        client.assertEncryptedCollectionCounts(collName, 5, 10, 5, 0);

        // Start delete which will keep retrying and fail due to write conflict
        const res = edb.runCommand({delete: collName, deletes: [{q: {_id: 6}, limit: 1}]});
        assert.commandFailedWithCode(
            res, ErrorCodes.WriteConflict, "delete did not fail with the expected code");

        // Unblock the compact
        assert.commandWorked(admin.runCommand({configureFailPoint: failpoint, mode: 'off'}));
        bgCompact();
        client.assertEncryptedCollectionCounts(collName, 5, 1, 2, 0);
    });

    jsTestLog("Testing two simultaneous compacts are serialized");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const coll = edb[collName];
        const failpoint1 = "fleCompactHangAfterESCPlaceholderInsert";
        const failpoint2 = "fleCompactFailBeforeECOCRead";

        for (let i = 1; i <= 5; i++) {
            assert.commandWorked(coll.deleteOne({_id: i}));
        }
        client.assertEncryptedCollectionCounts(collName, 5, 10, 5, 15);

        // Setup a failpoint that hangs after ESC placeholder insertion
        const hitCount =
            assert
                .commandWorked(admin.runCommand({configureFailPoint: failpoint1, mode: 'alwaysOn'}))
                .count;

        // Start the first compact, which hangs
        const bgCompactOne = startParallelShell(bgCompactFunc, conn.port);
        assert.commandWorked(admin.runCommand({
            waitForFailPoint: failpoint1,
            timesEntered: hitCount + 1,
            maxTimeMS: failpointWaitTimeoutMS
        }));

        // Enable the failpoint that throws on subsequent compacts
        assert.commandWorked(admin.runCommand({configureFailPoint: failpoint2, mode: 'alwaysOn'}));

        // Start the second compact which should not hit the throwing failpoint
        const bgCompactTwo = startParallelShell(bgCompactFunc, conn.port);

        // Not reliable, but need to delay so bgCompactTwo has a chance to actually send
        // the compact command, before the hanging failpoint is disabled.
        sleep(10 * 1000);

        // Disable the throwing failpoint
        assert.commandWorked(admin.runCommand({configureFailPoint: failpoint2, mode: 'off'}));

        // Unblock the first compact
        assert.commandWorked(admin.runCommand({configureFailPoint: failpoint1, mode: 'off'}));

        bgCompactOne();
        bgCompactTwo();

        client.assertEncryptedCollectionCounts(collName, 5, 1, 2, 0);
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
        client.assertEncryptedCollectionCounts(collName, 11, 1, 0, 1);
    });
}

function runTest(conn, primaryConn) {
    let shellArgs = [];
    // TODO: SERVER-73303 remove when v2 is enabled by default
    if (!isFLE2ProtocolVersion2Enabled()) {
        return;
    } else {
        shellArgs = ["--setShellParameter", "featureFlagFLE2ProtocolVersion2=true"];
    }

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
        const bgCompactOne = startParallelShell(bgCompactFunc, conn.port, false, ...shellArgs);

        // Wait until the compact hits the failpoint
        fp1.wait();

        // Enable the failpoint that throws on subsequent compacts
        const fp2 = configureFailPoint(primaryConn, failpoint2);

        // Start the second compact which should not hit the throwing failpoint
        const bgCompactTwo = startParallelShell(bgCompactFunc, conn.port, false, ...shellArgs);

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
        client.assertEncryptedCollectionCounts(collName, 10, 1, 0, 0);
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
        const bgCompactOne = startParallelShell(bgCompactFunc, conn.port, false, ...shellArgs);
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
        client.assertEncryptedCollectionCounts(collName, 11, 2, 0, 1);
    });
}

jsTestLog("ReplicaSet: Testing fle2 contention on compact");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTestV1(rst.getPrimary(), rst.getPrimary());
    runTest(rst.getPrimary(), rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 contention on compact");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});
    runTestV1(st.s, st.shard0);
    runTest(st.s, st.shard0);
    st.stop();
}
}());
