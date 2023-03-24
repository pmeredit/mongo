/**
 * Test FLE2 compact coordinator stepdown scenarios
 */
load("jstests/fle2/libs/encrypted_client_util.js");
load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallel_shell_helpers.js");
load("jstests/libs/uuid_util.js");

(function() {
'use strict';

// TODO: SERVER-74727 remove when v2 sharded compact is implemented
if (isFLE2ProtocolVersion2Enabled()) {
    jsTest.log("Test skipped because featureFlagFLE2ProtocolVersion2 is enabled");
    return;
}

const dbName = 'txn_compact_coordinator';
const collName = "basic";
const sampleEncryptedFields = {
    "fields": [
        {"path": "first", "bsonType": "string", "queries": {"queryType": "equality", contention: 0}}
    ]
};

const bgCompactFunc = function(assertWorked = true) {
    load("jstests/fle2/libs/encrypted_client_util.js");
    const client = new EncryptedClient(db.getMongo(), "txn_compact_coordinator");
    const result = client.getDB().basic.compact();
    if (assertWorked) {
        assert.commandWorked(result);
    }
};

function setupTest(client) {
    const edb = client.getDB();
    const coll = edb[collName];

    assert.commandWorked(edb.dropDatabase());

    let schemaCopy = JSON.parse(JSON.stringify(sampleEncryptedFields));
    assert.commandWorked(
        client.createEncryptionCollection(collName, {encryptedFields: schemaCopy}));

    // Insert data to compact
    for (let i = 1; i <= 10; i++) {
        assert.commandWorked(coll.insert({_id: i, "first": "mark"}));
    }
    client.assertEncryptedCollectionCounts(collName, 10, 10, 0, 10);
    return client;
}

function runCompactAndStepdownAtFailpoint(conn, fixture, failpoint) {
    let primary = fixture.rs0.getPrimary();
    let secondary = fixture.rs0.getSecondaries()[0];

    // Enable the hanging failpoint in the primary
    let primaryFp = configureFailPoint(primary, failpoint);

    // Start a compact, which hangs at the failpoint
    const bgCompact = startParallelShell(bgCompactFunc, conn.port);
    primaryFp.wait();

    // Step down the primary before disabling the failpoint
    fixture.rs0.stepUp(secondary);
    primaryFp.off();
    bgCompact();
}

// Tests that the coordinator resumes correctly when interrupted during the "drop" phase
function runStepdownDuringDropPhaseTest(conn, fixture) {
    const client = new EncryptedClient(conn, dbName);
    setupTest(client);
    jsTestLog("Testing stepdown during the drop phase");
    runCompactAndStepdownAtFailpoint(conn, fixture, "fleCompactHangAfterDropTempCollection");

    client.assertEncryptedCollectionCounts(collName, 10, 1, 0, 0);
    client.assertStateCollectionsAfterCompact(collName, true);
}

// Tests that the coordinator resumes correctly when interrupted by a stepdown
// during the "rename" phase, after it renamed the ECOC, but before creating
// the new ECOC.
//
// During step-up, the new primary will resume the ongoing compact operation;
// meanwhile, mongos will reissue the shardsvr compact command to the new primary.
// This reissue can result in two possible outcomes:
// 1. if the new primary receives the shardsvr command while resumed compact
//    operation is still unfinished, then it will combine the new command onto
//    the ongoing compact operation.  Since the resumed compact operation will
//    simply skip the phases after "rename" (because the catalog state no longer
//    matches the state document), the reissued shardsvr compact command effectively
//    results in a no-op. This results in a half-completed operation, and so
//    the client MUST resend the compact command to do the remaining work left
//    from the previous operation.
// 2. if the new primary receives the shardsvr command AFTER the resumed compact has
//    finished, then it is treated as a brand new compact operation, and the phases
//    after "rename" will not be skipped.
//
function runStepdownDuringRenamePhaseBeforeExplicitEcocCreate(conn, fixture) {
    const client = new EncryptedClient(conn, dbName);

    jsTestLog("Testing stepdown during the rename phase, before explicit ECOC create");
    setupTest(client);
    runCompactAndStepdownAtFailpoint(conn, fixture, "fleCompactHangBeforeECOCCreate");

    let renamedEcocExists = false;
    try {
        // check if the first compaction resulted in a half-complete state
        client.assertStateCollectionsAfterCompact(collName, false, true);
        renamedEcocExists = true;
    } catch (error) { /* ignore */
    }

    // Now, do the actual assertions
    client.assertStateCollectionsAfterCompact(collName, !renamedEcocExists, renamedEcocExists);

    if (renamedEcocExists) {
        // Retry the compact. Afterwards, the renamed ecoc should no longer exist.
        assert.commandWorked(client.getDB()[collName].compact());
        client.assertStateCollectionsAfterCompact(collName, true, false);
    }

    client.assertEncryptedCollectionCounts(collName, 10, 1, 0, 0);
}

// Tests that the coordinator resumes correctly when interrupted during the
// "rename" phase, after it renamed the ECOC and created the new ECOC.
function runStepdownDuringRenamePhaseAfterExplicitEcocCreate(conn, fixture) {
    const client = new EncryptedClient(conn, dbName);

    jsTestLog("Testing stepdown during the rename phase, after explicit ECOC create");
    setupTest(client);
    runCompactAndStepdownAtFailpoint(conn, fixture, "fleCompactHangAfterECOCCreate");

    let renamedEcocExists = false;
    try {
        // check if the first compaction resulted in a half-complete state
        client.assertStateCollectionsAfterCompact(collName, true, true);
        renamedEcocExists = true;
    } catch (error) { /* ignore */
    }

    client.assertStateCollectionsAfterCompact(collName, true, renamedEcocExists);

    if (renamedEcocExists) {
        // Retry the compact. Afterwards, the renamed ecoc should no longer exist.
        assert.commandWorked(client.getDB()[collName].compact());
        client.assertStateCollectionsAfterCompact(collName, true, false);
    }
    client.assertEncryptedCollectionCounts(collName, 10, 1, 0, 0);
}

// Tests that the coordinator resumes correctly when interrupted during the
// "rename" phase, after it created the new ECOC. This tests the case where the
// compact begins in a half-completed state, where the ECOC does not exist, but
// the temporary ecoc.compact collection does.
function runStepdownDuringRenamePhaseAfterExplicitEcocCreate_RenameSkipped(conn, fixture) {
    const client = new EncryptedClient(conn, dbName);

    jsTestLog(
        "Testing stepdown during the rename phase, after explicit ECOC create; ECOC does not exist");
    setupTest(client);

    // rename ecoc to ecoc.compact
    let ecocNss = client.getStateCollectionNamespaces(collName).ecoc;
    let ecocRenameNss = ecocNss + ".compact";
    assert.commandWorked(client.getDB()[ecocNss].renameCollection(ecocRenameNss));

    client.assertStateCollectionsAfterCompact(collName, false, true);

    // running compact again will create the ECOC collection, just before step down.
    runCompactAndStepdownAtFailpoint(conn, fixture, "fleCompactHangAfterECOCCreate");

    // What happens in the new primary upon step-up:
    // 1. New primary re-runs the rename phase. Since the ecoc.compact already exists
    //    and has the same UUID as before, it can proceed to the compact phase.
    // 2. It then checks if it needs to create the ECOC, but since the ECOC already exists,
    //    it skips the explicit create step.
    // 3. It then moves on to the compact phase.
    // What happens to the re-issued shardsvr command:
    // 1. if it arrives while the compact is in progress, it will join it.
    // 2. if it arrives after the resumed compact, then it starts a new compact, on an
    //    empty ECOC (i.e. no-op).
    // either way, the compaction succeeds without the need for the client to retry.
    client.assertStateCollectionsAfterCompact(collName, true, false);
    client.assertEncryptedCollectionCounts(collName, 10, 1, 0, 0);
}

{
    const rsOptions = {nodes: 2};
    const st = new ShardingTest({shards: {rs0: rsOptions}, mongos: 1, config: 1});
    assert.commandWorked(st.s.getDB(dbName).setLogLevel(5, "sharding"));

    runStepdownDuringDropPhaseTest(st.s, st);
    runStepdownDuringRenamePhaseBeforeExplicitEcocCreate(st.s, st);
    runStepdownDuringRenamePhaseAfterExplicitEcocCreate(st.s, st);
    runStepdownDuringRenamePhaseAfterExplicitEcocCreate_RenameSkipped(st.s, st);
    st.stop();
}
}());
