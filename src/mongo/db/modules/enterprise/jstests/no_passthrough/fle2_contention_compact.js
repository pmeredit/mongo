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
    "fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
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

function runTest(conn, primaryConn) {
    const testDb = conn.getDB(dbName);
    const admin = primaryConn.getDB("admin");
    const isMongos = conn.isMongos();

    assert.commandWorked(testDb.setLogLevel(5, "sharding"));

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
        admin.runCommand(
            {waitForFailPoint: failpoint, timesEntered: hitCount + 1, maxTimeMS: 10000});
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
        admin.runCommand(
            {waitForFailPoint: failpoint, timesEntered: hitCount + 1, maxTimeMS: 10000});
        client.assertEncryptedCollectionCounts(collName, 10, 11, 0, 0);

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
        admin.runCommand(
            {waitForFailPoint: failpoint, timesEntered: hitCount + 1, maxTimeMS: 10000});

        // ESC now has the placeholder at this point
        client.assertEncryptedCollectionCounts(collName, 5, 11, 5, 0);

        // Delete another entry, which inserts an ECC entry in the next position: 6
        assert.commandWorked(coll.deleteOne({_id: 6}));
        client.assertEncryptedCollectionCounts(collName, 4, 11, 6, 1);

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
        admin.runCommand(
            {waitForFailPoint: failpoint, timesEntered: hitCount + 1, maxTimeMS: 10000});
        // ESC and ECC now have placeholders at this point
        client.assertEncryptedCollectionCounts(collName, 5, 11, 6, 0);

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
        const failpoint = "fleCompactHangAfterESCPlaceholderInsert";

        for (let i = 1; i <= 5; i++) {
            assert.commandWorked(coll.deleteOne({_id: i}));
        }
        client.assertEncryptedCollectionCounts(collName, 5, 10, 5, 15);

        // Setup a failpoint that hangs after ESC placeholder insertion
        const hitCount =
            assert
                .commandWorked(admin.runCommand({configureFailPoint: failpoint, mode: 'alwaysOn'}))
                .count;

        // Start the first compact, which hangs
        const bgCompactOne = startParallelShell(bgCompactFunc, conn.port);
        admin.runCommand(
            {waitForFailPoint: failpoint, timesEntered: hitCount + 1, maxTimeMS: 10000});

        // Start the second compact, which waits for the first compact
        const bgCompactTwo = startParallelShell(bgCompactFunc, conn.port);

        // Unblock the first compact
        assert.commandWorked(admin.runCommand({configureFailPoint: failpoint, mode: 'off'}));
        bgCompactOne();
        bgCompactTwo();

        // the second compact should be a no-op as there is nothing to compact
        const expectedLogId = isMongos ? 6548305 : 6548306;
        checkLog.containsJson(primaryConn, expectedLogId);
        client.assertEncryptedCollectionCounts(collName, 5, 1, 2, 0);
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
