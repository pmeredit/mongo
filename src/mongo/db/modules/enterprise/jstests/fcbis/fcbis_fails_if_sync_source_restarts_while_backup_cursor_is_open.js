/**
 * Tests that file copy based initial sync will fail if sync source restarts while backup cursor is
 * open in either the original backup phase or the extend backup phase
 * @tags: [requires_persistence, requires_wiredtiger]
 */
import {kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

function runTest(failpoint) {
    TestData.skipEnforceFastCountOnValidate = true;
    const testName = "fcbis_fails_if_sync_source_restarts_while_backup_cursor_is_open";
    const rst = new ReplSetTest({
        name: testName,
        nodes: 1,
        // TODO: BF-23748 Lower the initial sync log level once the BF is resolved.
        nodeOptions: {
            setParameter: {logComponentVerbosity: tojson({replication: {initialSync: 3}})},
        },
    });
    rst.startSet();
    rst.initiate();
    const primary = rst.getPrimary();
    const primaryDb = primary.getDB("test");

    // Add some data to be cloned.
    assert.commandWorked(primaryDb.test.insert([{a: 1}, {b: 2}, {c: 3}]));
    rst.awaitReplication();

    jsTestLog("Adding the initial sync destination node to the replica set");
    const initialSyncNode = rst.add({
        rsConfig: {priority: 0, votes: 0},
        setParameter: {
            'initialSyncMethod': 'fileCopyBased',
            'numInitialSyncAttempts': 1,
            // TODO: BF-23748 Lower the initial sync log level once the BF is resolved.
            'logComponentVerbosity': tojson({replication: {verbosity: 1, initialSync: 3}}),
            'failpoint.fCBISHangBeforeFinish': tojson({mode: 'alwaysOn'}),
        }
    });

    // Hang while the backup cursor is open in either the original backup phase or the extend
    // backup phase.
    assert.commandWorked(
        initialSyncNode.adminCommand({configureFailPoint: failpoint, mode: "alwaysOn"}));

    if (failpoint === "fCBISHangAfterExtendingBackupCursor") {
        assert.commandWorked(initialSyncNode.adminCommand(
            {configureFailPoint: 'fCBISForceExtendBackupCursor', mode: "alwaysOn"}));
    }

    jsTestLog("Attempting file copy based initial sync");
    rst.reInitiate();

    if (failpoint === "fCBISHangAfterExtendingBackupCursor") {
        jsTestLog("Extending the backup cursor");
        assert.commandWorked(initialSyncNode.adminCommand({
            waitForFailPoint: 'fCBISForceExtendBackupCursor',
            timesEntered: 1,
            maxTimeMS: kDefaultWaitForFailPointTimeout
        }));

        assert.commandWorked(initialSyncNode.adminCommand(
            {configureFailPoint: 'fCBISForceExtendBackupCursor', mode: "off"}));
    }

    assert.commandWorked(initialSyncNode.adminCommand({
        waitForFailPoint: failpoint,
        timesEntered: 1,
        maxTimeMS: kDefaultWaitForFailPointTimeout
    }));

    jsTestLog("Restarting the sync source after reaching " + failpoint + " failpoint");
    rst.restart(primary);

    assert.commandWorked(
        initialSyncNode.adminCommand({configureFailPoint: failpoint, mode: "off"}));

    jsTestLog("File copy based initial sync should fail");
    // File copy based initial sync reaches an error in the backup file cloner stage
    checkLog.containsJson(initialSyncNode, 21077);
    checkLog.containsJson(initialSyncNode, 5781900);

    assert.adminCommandWorkedAllowingNetworkError(
        initialSyncNode, {configureFailPoint: "fCBISHangBeforeFinish", mode: "off"});
    assert.eq(MongoRunner.EXIT_ABRUPT, waitMongoProgram(initialSyncNode.port));

    // We skip validation and dbhashes because the initial sync failed so the initial sync node is
    // invalid and unreachable.
    TestData.skipCheckDBHashes = true;
    rst.stopSet(null, null, {skipValidation: true});
}

runTest("fCBISHangAfterOpeningBackupCursor");
runTest("fCBISHangAfterExtendingBackupCursor");
