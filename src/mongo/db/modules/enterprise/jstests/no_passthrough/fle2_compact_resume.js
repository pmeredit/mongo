/**
 * Test FLE2 compact works when resumed
 *
 * @tags: [
 * ]
 */
import {runEncryptedTest} from "jstests/fle2/libs/encrypted_client_util.js";
import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

const dbName = 'txn_compact_resume';
const collName = "basic";
const sampleEncryptedFields = {
    "fields": [
        {"path": "first", "bsonType": "string", "queries": {"queryType": "equality", contention: 0}}
    ]
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
    const isMongos = conn.isMongos();

    jsTestLog("Test compact behavior when temp ECOC drop is skipped and compact is resumed");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const coll = edb[collName];
        const failpoint = isMongos ? "fleCompactSkipECOCDrop" : "fleCompactSkipECOCDropUnsharded";

        // enable failpoint to skip the drop phase
        const fp = configureFailPoint(primaryConn, failpoint);

        // run compact
        assert.commandWorked(coll.compact());
        client.assertStateCollectionsAfterCompact(
            collName, true /* ecocExists */, true /* ecocRenameExists */);

        // 1 anchor inserted, and 10 non-anchors removed
        client.assertEncryptedCollectionCounts(collName, 10, 1, 0);

        fp.off();

        // re-run compact
        assert.commandWorked(coll.compact());
        client.assertStateCollectionsAfterCompact(collName, true, false);

        // no new anchors should have been added
        client.assertEncryptedCollectionCounts(collName, 10, 1, 0);
    });

    jsTestLog("Test compact does not delete new non-anchor entries on resume");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const coll = edb[collName];
        const failpoint = isMongos ? "fleCompactSkipECOCDrop" : "fleCompactSkipECOCDropUnsharded";

        // enable failpoint to skip the drop phase
        const fp = configureFailPoint(primaryConn, failpoint);

        // run compact
        assert.commandWorked(coll.compact());
        client.assertStateCollectionsAfterCompact(
            collName, true /* ecocExists */, true /* ecocRenameExists */);

        // 1 anchor inserted, and 10 non-anchors removed
        client.assertEncryptedCollectionCounts(collName, 10, 1, 0);
        fp.off();

        // insert 10 with a different value (adds new non-anchors to ESC)
        for (let i = 1; i <= 10; i++) {
            assert.commandWorked(coll.insert({"first": "bob"}));
        }
        client.assertEncryptedCollectionCounts(collName, 20, 11, 10);

        // re-run compact
        assert.commandWorked(coll.compact());
        client.assertStateCollectionsAfterCompact(collName, true, false);

        // assert that it:
        // 1. resumed from the existing ecocRename collection
        // 2. did not add new anchors (because the entries in ecocRename are for the old value)
        // 3. did not delete non-anchors from ESC
        client.assertEncryptedCollectionCounts(collName, 20, 11, 10);

        // compact the new values
        assert.commandWorked(coll.compact());
        client.assertStateCollectionsAfterCompact(collName, true, false);
        client.assertEncryptedCollectionCounts(collName, 20, 2, 0);
    });
}

jsTestLog("ReplicaSet: Testing fle2 compact resume");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary(), rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 compact resume");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});
    runTest(st.s, st.shard0);
    st.stop();
}
