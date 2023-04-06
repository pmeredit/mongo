/**
 * Test FLE2 compact works with concurrent FCV changes
 *
 * @tags: [
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");
load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallel_shell_helpers.js");

(function() {
'use strict';

const dbName = 'txn_compact';
const collName = "basic";
const sampleEncryptedFields = {
    "fields": [
        {"path": "first", "bsonType": "string", "queries": {"queryType": "equality", contention: 0}}
    ]
};

const bgCompactFunc = function() {
    load("jstests/fle2/libs/encrypted_client_util.js");
    const client = new EncryptedClient(db.getMongo(), "txn_compact");
    assert.commandWorked(client.getDB().basic.compact());
};

const bgSetFCVFunc = function(targetFCV) {
    print("Setting FCV to " + targetFCV);
    const result =
        assert.commandWorked(db.adminCommand({setFeatureCompatibilityVersion: targetFCV}));
    print("Set FCV result: " + tojson(result));
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

function testSetFCVIsSerialized(conn, rstFixture) {
    const testDb = conn.getDB(dbName);
    const primaryConn = rstFixture.getPrimary();
    const adminDb = conn.getDB("admin");

    jsTestLog("Testing FCV change cannot happen while in the middle of a compact transaction");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        // Setup a failpoint that hangs before ESC anchor insertion (inside a TXN)
        const failpoint = "fleCompactHangBeforeESCAnchorInsert";
        const fp = configureFailPoint(primaryConn, failpoint);

        // Start the first compact. Wait until it hits the failpoint.
        const bgCompact = startParallelShell(bgCompactFunc, conn.port);
        fp.wait();

        // Set the FCV in the background
        const bgSetFCV =
            startParallelShell(funWithArgs(bgSetFCVFunc, lastContinuousFCV), conn.port);
        sleep(5000);

        if (!isMongos(adminDb)) {
            // If unsharded, FCV should still be latest.
            checkFCV(adminDb, latestFCV);
        } else {
            // Otherwise, FCV is in a transitioning state where:
            // - version is set to the target version (lastContinuous)
            // - and previousVersion is set to latest.
            checkFCV(adminDb, lastContinuousFCV, lastContinuousFCV);
        }

        // Unblock compact
        fp.off();
        bgCompact();
        bgSetFCV();

        // FCV is lastContinuous
        checkFCV(adminDb, lastContinuousFCV);

        // reset FCV to latest
        assert.commandWorked(adminDb.runCommand({setFeatureCompatibilityVersion: latestFCV}));
        checkFCV(adminDb, latestFCV);
    });
}

jsTestLog("ReplicaSet: Testing fle2 compact blocks setFCV");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();
    rst.initiate();
    rst.awaitReplication();

    testSetFCVIsSerialized(rst.getPrimary(), rst);

    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 compact drains before setFCV completes");
{
    const rsOptions = {nodes: 2};
    const st = new ShardingTest({shards: {rs0: rsOptions}, mongos: 1, config: 1});

    st.forEachConnection(
        (conn) => { assert.commandWorked(conn.getDB(dbName).setLogLevel(1, "sharding")); });

    testSetFCVIsSerialized(st.s, st.rs0);

    st.stop();
}
}());
