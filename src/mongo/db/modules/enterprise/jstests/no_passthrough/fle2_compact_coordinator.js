/**
 * Test FLE2 compact coordinator stepdown scenarios
 */
load("jstests/fle2/libs/encrypted_client_util.js");
load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallel_shell_helpers.js");
load("jstests/libs/uuid_util.js");

(function() {
'use strict';

const dbName = 'txn_compact_coordinator';
const collName = "basic";
const sampleEncryptedFields = {
    "fields": [
        {"path": "first", "bsonType": "string", "queries": {"queryType": "equality", contention: 0}}
    ]
};

const bgCompactFunc = function() {
    load("jstests/fle2/libs/encrypted_client_util.js");
    const client = new EncryptedClient(db.getMongo(), "txn_compact_coordinator");
    const result = assert.commandWorked(client.getDB().basic.compact());
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

// Tests that the coordinator resumes correctly when interrupted during the "drop" phase
function runStepdownDuringDropPhaseTest(conn, fixture) {
    const testDb = conn.getDB(dbName);

    assert.commandWorked(testDb.setLogLevel(5, "sharding"));

    jsTestLog("Testing stepdown during the drop phase");
    runEncryptedTest(testDb, dbName, collName, sampleEncryptedFields, (edb, client) => {
        setupTest(client);

        const coll = edb[collName];
        const failpoint = "fleCompactHangAfterDropTempCollection";

        let primary = fixture.rs0.getPrimary();
        let secondary = fixture.rs0.getSecondaries()[0];

        // In the primary, hang after the temp ecoc collection is dropped
        let primaryFp = configureFailPoint(primary, failpoint);

        // Start a compact, which will hang after the temp ecoc collection is dropped
        const bgCompact = startParallelShell(bgCompactFunc, conn.port);
        primaryFp.wait();

        // Create the ecoc collection; this is so that when the shardsvr
        // compactStructuredEncryptionData is reissued on step-up, it doesn't
        // just return immediately due to the absence of both the ecoc and ecoc.compact
        // collections.
        assert.commandWorked(edb.createCollection("enxcol_.basic.ecoc"));

        // step down the primary before the coordinator wraps up
        fixture.rs0.stepUp(secondary);
        primaryFp.off();

        bgCompact();

        client.assertEncryptedCollectionCounts(collName, 10, 1, 0, 0);
    });
}

{
    const rsOptions = {nodes: 2};
    const st = new ShardingTest({shards: {rs0: rsOptions}, mongos: 1, config: 1});
    runStepdownDuringDropPhaseTest(st.s, st);
    st.stop();
}
}());
