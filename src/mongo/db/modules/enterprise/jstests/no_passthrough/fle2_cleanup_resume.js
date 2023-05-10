/**
 * Test FLE2 cleanup works when resumed
 *
 * @tags: [
 * featureFlagFLE2CleanupCommand
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");
load("jstests/libs/fail_point_util.js");

(function() {
'use strict';

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
        client.assertStateCollectionsAfterCompact(collName,
                                                  true /* ecocExists */,
                                                  true /* ecocRenameExists */,
                                                  true /* escDeletesExists */);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // disable the first failpoint
        fp1.off();

        // enable failpoint that fails before the ECOC reads
        const fp2 = configureFailPoint(primaryConn, failpoint2);

        // Cleanup #2
        assert.commandFailedWithCode(coll.cleanup(), failpoint2Code);

        // 2nd cleanup removed half (9) of the anchors (contents of the previous 'esc.deletes')
        anchorCount = anchorCount / 2;
        client.assertStateCollectionsAfterCompact(collName, true, true, true);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // disable the second failpoint
        fp2.off();

        // Cleanup #3
        assert.commandWorked(coll.cleanup());

        // 3rd cleanup inserted 1 null anchor for the other value, and removed all anchors.
        // But, it did not delete non-anchors because it resumed from a previous 'ecoc.compact'.
        nullAnchorCount++;
        anchorCount = 0;
        client.assertStateCollectionsAfterCompact(collName, true, false, false);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // Cleanup #4
        assert.commandWorked(coll.cleanup());

        // 4th cleanup deleted non-anchors
        nonAnchorCount = 0;
        client.assertStateCollectionsAfterCompact(collName, true, false, false);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, 0);
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
        nullAnchorCount++;
        client.assertStateCollectionsAfterCompact(collName,
                                                  true /* ecocExists */,
                                                  true /* ecocRenameExists */,
                                                  true /* escDeletesExists */);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);
        fp1.off();

        // run compact on the 'ecoc.compact' left behind by the previous cleanup
        assert.commandWorked(coll.compact());
        client.assertStateCollectionsAfterCompact(collName, true, false, true);
        // compact added 1 anchor, removed nothing
        anchorCount++;
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // run 2nd compact to remove non-anchors (20)
        assert.commandWorked(coll.compact());
        client.assertStateCollectionsAfterCompact(collName, true, false, true);
        nonAnchorCount = 0;
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // run 2nd cleanup to delete anchors (9) recorded in 'esc.deletes' from the previous cleanup
        // no compaction occurs because ECOC is empty.
        assert.commandWorked(coll.cleanup());
        client.assertStateCollectionsAfterCompact(collName, true, false, false);
        anchorCount -= 9;
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // insert one for each unique value
        assert.commandWorked(coll.insert({"first": "jack"}));
        assert.commandWorked(coll.insert({"first": "diane"}));
        nonAnchorCount += 2;
        client.assertEncryptedCollectionCounts(collName, 202, expectedESCCount(), 2);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // run 3rd cleanup to compact all anchors & nonanchors
        assert.commandWorked(coll.cleanup());
        nullAnchorCount++;
        anchorCount = 0;
        nonAnchorCount = 0;
        client.assertStateCollectionsAfterCompact(collName, true, false, false);
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
        client.assertStateCollectionsAfterCompact(collName,
                                                  true /* ecocExists */,
                                                  true /* ecocRenameExists */,
                                                  false /* escDeletesExists */);
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);
        fp1.off();

        // run cleanup on the 'ecoc.compact' left behind by the previous compact
        assert.commandWorked(coll.cleanup());
        client.assertStateCollectionsAfterCompact(collName, true, false, false);
        // cleanup added 2 null anchors, removed 19 anchors; does not delete non-anchors due to
        // pre-existing 'ecoc.compact'.
        nullAnchorCount += 2;
        anchorCount -= 19;
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
        client.assertESCNonAnchorCount(collName, nonAnchorCount);

        // run 2nd compact to remove non-anchors (20)
        assert.commandWorked(coll.compact());
        nonAnchorCount = 0;
        client.assertStateCollectionsAfterCompact(collName, true, false, false);
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
    // TODO: SERVER-76479 uncomment when sharded cleanup is done
    // const st = new ShardingTest({shards: 1, mongos: 1, config: 1});
    // runTest(st.s, st.shard0);
    // st.stop();
}
}());
