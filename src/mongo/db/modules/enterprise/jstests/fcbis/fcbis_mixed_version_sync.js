/**
 * Tests that file copy based initial sync succeeds with mixed FCV replica sets. The
 * 'runDowngradeTest()' function starts a replica set, sets the FCV to a downgraded version, adds a
 * new node in the latest FCV, and ensures that the newly added node ends up in the downgraded FCV.
 * The only way to test initial syncing a downgraded FCV node to an upgraded FCV replica set is with
 * a sharded cluster, as shard servers start in downgraded FCV by default. As a result,
 * 'runUpgradeTest()' starts a sharded cluster, adds a downgraded FCV node to the shard, and ensures
 * the newly added node ends up in the latest FCV.
 * @tags: [multiversion_incompatible, requires_persistence, requires_wiredtiger]
 */

import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

function runDowngradeTest(downgradeFCV) {
    TestData.skipEnforceFastCountOnValidate = true;
    const rst = new ReplSetTest({
        nodes: 2,
    });
    rst.startSet();
    rst.initiateWithHighElectionTimeout();

    const primary = rst.getPrimary();
    const secondary = rst.getSecondary();

    jsTestLog("Setting FCV to " + downgradeFCV);
    assert.commandWorked(
        primary.adminCommand({setFeatureCompatibilityVersion: downgradeFCV, confirm: true}));

    checkFCV(primary.getDB("admin"), downgradeFCV);
    checkFCV(secondary.getDB("admin"), downgradeFCV);

    // Add some data to be synced.
    assert.commandWorked(primary.getDB("testDB").test.insert([{a: 1}, {b: 2}, {c: 3}]));
    rst.awaitReplication();
    // Ensure there's an up-to-date stable checkpoint for FCBIS to copy.
    rst.awaitLastStableRecoveryTimestamp();
    assert.commandWorked(primary.adminCommand({fsync: 1}));

    jsTestLog(
        "Adding the initial sync destination node with the latest FCV to the downgraded FCV replica set");
    const initialSyncNode = rst.add({
        setParameter: {
            'initialSyncMethod': 'fileCopyBased',
            'logComponentVerbosity': tojson({replication: {verbosity: 1, initialSync: 2}}),
            'failpoint.forceSyncSourceCandidate':
                tojson({mode: 'alwaysOn', data: {hostAndPort: primary.host}})
        }
    });

    rst.reInitiate();
    rst.waitForState(initialSyncNode, ReplSetTest.State.SECONDARY);
    const initialSyncNodeDB = initialSyncNode.getDB("testDB");

    assert.eq(3, initialSyncNodeDB.test.find().itcount());
    checkFCV(initialSyncNode.getDB("admin"), downgradeFCV);
    rst.stopSet();
}

function runUpgradeTest() {
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});

    const rst = st.rs0;
    const primary = st.rs0.getPrimary();
    const db = st.getDB("testDB");
    // Add some data to be synced.
    assert.commandWorked(db.test.insert([{a: 1}, {b: 2}, {c: 3}]));
    // Ensure there's an up-to-date stable checkpoint for FCBIS to copy.
    rst.awaitLastStableRecoveryTimestamp();
    assert.commandWorked(primary.adminCommand({fsync: 1}));

    checkFCV(primary.getDB("admin"), latestFCV);

    jsTestLog(
        "Adding the initial sync destination node with downgraded FCV to the latest FCV shard replica set");
    const initialSyncNode =
        rst.add({'shardsvr': '', setParameter: {'initialSyncMethod': 'fileCopyBased'}});
    rst.reInitiate();
    rst.waitForState(initialSyncNode, ReplSetTest.State.SECONDARY);

    const initialSyncNodeDB = initialSyncNode.getDB("testDB");
    assert.eq(3, initialSyncNodeDB.test.find().itcount());
    checkFCV(initialSyncNode.getDB("admin"), latestFCV);
    st.stop();
}

runDowngradeTest(lastLTSFCV);
if (lastLTSFCV !== lastContinuousFCV) {
    runDowngradeTest(lastContinuousFCV);
}
runUpgradeTest();
