/**
 * Test FLE2 cleanup works when resumed
 *
 * @tags: [
 * requires_fcv_71
 * ]
 */
import {runEncryptedTest} from "jstests/fle2/libs/encrypted_client_util.js";
import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

const dbName = 'fle2_cleanup_resume';
const collName = "basic";
const sampleEncryptedFields = {
    "fields": [
        {"path": "first", "bsonType": "string", "queries": {"queryType": "equality", contention: 0}}
    ]
};

let anchorCount = 0;
let nonAnchorCount = 0;
let nullAnchorCount = 0;

function expectedESCCount() {
    return anchorCount + nonAnchorCount + nullAnchorCount;
}

function setupTest(client) {
    const coll = client.getDB()[collName];

    for (let cycle = 0; cycle < 10; cycle++) {
        // Insert data to compact
        for (let i = 1; i <= 10; i++) {
            assert.commandWorked(coll.insert({"first": "jack"}));
        }
        for (let i = 1; i <= 10; i++) {
            assert.commandWorked(coll.insert({"first": "diane"}));
        }
        // compact insertions, except for last iteration
        if ((cycle + 1) < 10) {
            assert.commandWorked(coll.compact());
        }
    }

    // assert 20 nonanchors inserted from the last cycle + 9 anchors per value
    nonAnchorCount = 20;
    anchorCount = 2 * 9;
    nullAnchorCount = 0;
    client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 20);
    return client;
}

function runTest(conn, primaryConn) {
    const testDb = conn.getDB(dbName);

    jsTestLog("Test cleanup resume at various failpoints");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const coll = edb[collName];
        const failpoint1 = "fleCleanupFailAfterTransactionCommit";
        const failpoint2 = "fleCompactOrCleanupFailBeforeECOCRead";
        const failpoint1Code = 7663002;
        const failpoint2Code = 7293605;

        // enable failpoint to fail after one transaction commit
        const fp1 = configureFailPoint(primaryConn, failpoint1);

        // Cleanup #1
        assert.commandFailedWithCode(coll.cleanup(), failpoint1Code);

        // 1st cleanup inserted 1 null anchor, and deleted nothing
        nullAnchorCount++;
        client.assertStateCollectionsAfterCompact(
            collName, true /* ecocExists */, true /* ecocRenameExists */);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // disable the first failpoint
        fp1.off();

        // enable failpoint that fails before the ECOC reads
        const fp2 = configureFailPoint(primaryConn, failpoint2);

        // Cleanup #2
        assert.commandFailedWithCode(coll.cleanup(), failpoint2Code);

        // 2nd cleanup removed none of the anchors
        client.assertStateCollectionsAfterCompact(collName, true, true);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // disable the second failpoint
        fp2.off();

        // Cleanup #3
        assert.commandWorked(coll.cleanup());

        // 3rd cleanup inserted 1 null anchor for the other value, and removed its anchors.
        // But, it did not delete non-anchors because it resumed from a previous 'ecoc.compact'.
        nullAnchorCount++;
        anchorCount = anchorCount / 2;
        client.assertStateCollectionsAfterCompact(collName, true, false);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // Cleanup #4
        assert.commandWorked(coll.cleanup());

        // 4th cleanup deleted non-anchors
        nonAnchorCount = 0;
        client.assertStateCollectionsAfterCompact(collName, true, false);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, 0);
    });

    jsTestLog("Test compacted anchors can no longer be deleted on resume after failure");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const coll = edb[collName];
        const failpoint = "fleCleanupFailDuringAnchorDeletes";
        const failpointCode = 7723800;

        // enable failpoint to throw just before anchors are removed
        const fp = configureFailPoint(primaryConn, failpoint);
        assert.commandFailedWithCode(coll.cleanup(), failpointCode);

        // cleanup inserts 2 null anchors, and deletes all non-anchors before failing
        nullAnchorCount += 2;
        nonAnchorCount = 0;
        client.assertStateCollectionsAfterCompact(collName, true, true);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);
        fp.off();

        // re-run cleanup
        assert.commandWorked(coll.cleanup());

        client.assertStateCollectionsAfterCompact(collName, true, false);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);
    });

    jsTestLog("Test compact after a partial cleanup");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const coll = edb[collName];
        const failpoint = "fleCleanupFailAfterTransactionCommit";
        const failpointCode = 7663002;

        // enable failpoint to fail after one transaction commit
        const fp1 = configureFailPoint(primaryConn, failpoint);

        // run cleanup, which throws at the failpoint
        assert.commandFailedWithCode(coll.cleanup(), failpointCode);

        // cleanup inserted 1 null anchor, and deleted nothing
        // Half of all anchors are now unremovable anchors
        let unremovableAnchorCount = 9;
        nullAnchorCount++;
        client.assertStateCollectionsAfterCompact(
            collName, true /* ecocExists */, true /* ecocRenameExists */);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);
        fp1.off();

        // run compact on the 'ecoc.compact' left behind by the previous cleanup
        assert.commandWorked(coll.compact());
        client.assertStateCollectionsAfterCompact(collName, true, false);
        // compact added 1 anchor, removed nothing
        anchorCount++;
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // run 2nd compact to remove non-anchors (20)
        assert.commandWorked(coll.compact());
        client.assertStateCollectionsAfterCompact(collName, true, false);
        nonAnchorCount = 0;
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // running a 2nd cleanup does nothing because ECOC is empty.
        assert.commandWorked(coll.cleanup());
        client.assertStateCollectionsAfterCompact(collName, true, false);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // insert one for each unique value
        assert.commandWorked(coll.insert({"first": "jack"}));
        assert.commandWorked(coll.insert({"first": "diane"}));
        nonAnchorCount += 2;
        client.assertEncryptedCollectionCounts(collName, 202, expectedESCCount(), 2);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // run 3rd cleanup to compact all removable anchors & nonanchors
        assert.commandWorked(coll.cleanup());
        nullAnchorCount++;
        anchorCount = unremovableAnchorCount;
        nonAnchorCount = 0;
        client.assertStateCollectionsAfterCompact(collName, true, false);
        client.assertEncryptedCollectionCounts(collName, 202, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);
    });

    jsTestLog("Test cleanup after a partial compact");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const coll = edb[collName];
        const failpoint = "fleCompactFailAfterTransactionCommit";
        const failpointCode = 7663001;

        // enable failpoint to fail after one transaction commit
        const fp1 = configureFailPoint(primaryConn, failpoint);

        // run compact, which throws at the failpoint
        assert.commandFailedWithCode(coll.compact(), failpointCode);

        // compact inserted 1 anchor, and deleted nothing
        anchorCount++;
        client.assertStateCollectionsAfterCompact(
            collName, true /* ecocExists */, true /* ecocRenameExists */);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);
        fp1.off();

        // run cleanup on the 'ecoc.compact' left behind by the previous compact
        assert.commandWorked(coll.cleanup());
        client.assertStateCollectionsAfterCompact(collName, true, false);
        // cleanup added 2 null anchors, removed 19 anchors; does not delete non-anchors due to
        // pre-existing 'ecoc.compact'.
        nullAnchorCount += 2;
        anchorCount -= 19;
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // run 2nd compact to remove non-anchors (20)
        assert.commandWorked(coll.compact());
        nonAnchorCount = 0;
        client.assertStateCollectionsAfterCompact(collName, true, false);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);
    });
}

jsTestLog("ReplicaSet: Testing fle2 cleanup resume");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary(), rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 cleanup resume");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});
    runTest(st.s, st.shard0);
    st.stop();
}
