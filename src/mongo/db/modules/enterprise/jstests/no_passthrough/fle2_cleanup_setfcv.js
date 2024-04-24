/**
 * Test FLE2 cleanup works with concurrent FCV changes
 *
 * @tags: [
 * requires_fcv_71
 * ]
 */
import {runEncryptedTest} from "jstests/fle2/libs/encrypted_client_util.js";
import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {funWithArgs} from "jstests/libs/parallel_shell_helpers.js";

const dbName = 'fle2_cleanup_setfcv';
const collName = "basic";
const sampleEncryptedFields = {
    "fields": [
        {"path": "first", "bsonType": "string", "queries": {"queryType": "equality", contention: 0}}
    ]
};

const bgCleanupFunc = async function() {
    const {EncryptedClient} = await import("jstests/fle2/libs/encrypted_client_util.js");
    const client = new EncryptedClient(db.getMongo(), "fle2_cleanup_setfcv");
    client.runEncryptionOperation(() => { assert.commandWorked(client.getDB().basic.cleanup()); });
};

const bgSetFCVFunc = function(targetFCV) {
    print("Setting FCV to " + targetFCV);
    const result = assert.commandWorked(
        db.adminCommand({setFeatureCompatibilityVersion: targetFCV, confirm: true}));
    print("Set FCV result: " + tojson(result));
};

function setupTest(client) {
    const coll = client.getDB()[collName];

    // Insert data to cleanup
    for (let i = 1; i <= 10; i++) {
        assert.commandWorked(coll.insert({_id: i, "first": "mark"}));
    }
    client.assertEncryptedCollectionCounts(collName, 10, 10, 10);
    return client;
}

function testSetFCVIsSerialized(conn, rstFixture) {
    const testDb = conn.getDB(dbName);
    const primaryConn = rstFixture.getPrimary();
    const adminDb = conn.getDB("admin");

    jsTestLog("Testing FCV change cannot happen while in the middle of a cleanup transaction");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        // Setup a failpoint that hangs before ESC null anchor update/insert (inside a TXN)
        const failpoint = "fleCleanupHangBeforeNullAnchorUpdate";
        const fp = configureFailPoint(primaryConn, failpoint);

        // Start the first cleanup. Wait until it hits the failpoint.
        const bgCleanup = startParallelShell(bgCleanupFunc, conn.port);
        fp.wait();

        // Set the FCV in the background
        const bgSetFCV =
            startParallelShell(funWithArgs(bgSetFCVFunc, lastContinuousFCV), conn.port);
        sleep(5000);

        // If unsharded, FCV should still be latest.
        checkFCV(adminDb, latestFCV);

        // Unblock cleanup
        fp.off();
        bgCleanup();
        bgSetFCV();

        // FCV is lastContinuous
        checkFCV(adminDb, lastContinuousFCV);

        // reset FCV to latest
        assert.commandWorked(
            adminDb.runCommand({setFeatureCompatibilityVersion: latestFCV, confirm: true}));
        checkFCV(adminDb, latestFCV);
    });
}

jsTestLog("ReplicaSet: Testing fle2 cleanup blocks setFCV");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();
    rst.initiate();
    rst.awaitReplication();

    testSetFCVIsSerialized(rst.getPrimary(), rst);

    rst.stopSet();
}
