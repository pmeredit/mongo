/**
 * Test FLE2 cleanup coordinator stepdown scenarios
 *
 * @tags: [
 * requires_fcv_71
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {configureFailPoint} from "jstests/libs/fail_point_util.js";

const dbName = 'fle2_cleanup_coordinator';
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

const bgCleanupFunc = async function(assertWorked = true) {
    const {EncryptedClient} = await import("jstests/fle2/libs/encrypted_client_util.js");
    const client = new EncryptedClient(db.getMongo(), "fle2_cleanup_coordinator");
    client.runEncryptionOperation(() => {
        const result = client.getDB().basic.cleanup();

        if (assertWorked) {
            assert.commandWorked(result);
        }
    });
};

function setupTest(client) {
    const edb = client.getDB();
    const coll = edb[collName];

    assert.commandWorked(edb.dropDatabase());

    let schemaCopy = JSON.parse(JSON.stringify(sampleEncryptedFields));
    assert.commandWorked(
        client.createEncryptionCollection(collName, {encryptedFields: schemaCopy}));

    for (let cycle = 0; cycle < 10; cycle++) {
        // Insert data to compact
        for (let i = 1; i <= 10; i++) {
            assert.commandWorked(coll.einsert({"first": "jack"}));
        }
        for (let i = 1; i <= 10; i++) {
            assert.commandWorked(coll.einsert({"first": "diane"}));
        }
        // compact insertions, except for last iteration
        if ((cycle + 1) < 10) {
            client.runEncryptionOperation(() => { assert.commandWorked(coll.compact()); });
        }
    }

    // assert 20 anchors inserted from the last cycle + 9 anchors per value
    nonAnchorCount = 20;
    anchorCount = 2 * 9;
    nullAnchorCount = 0;
    client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 20);
    return client;
}

function runCleanupAndStepdownAtFailpoint(conn, fixture, failpoint) {
    let primary = fixture.rs0.getPrimary();
    let secondary = fixture.rs0.getSecondaries()[0];

    // Enable the hanging failpoint in the primary
    let primaryFp = configureFailPoint(primary, failpoint);

    // Start a cleanup, which hangs at the failpoint
    const bgCleanup = startParallelShell(bgCleanupFunc, conn.port);
    primaryFp.wait();

    // Step down the primary before disabling the failpoint
    fixture.rs0.stepUp(secondary);
    primaryFp.off();
    bgCleanup();
}

// Tests that the coordinator resumes correctly when interrupted by a stepdown
// during the "rename" phase, after it renamed the ECOC, but before creating
// the new ECOC.
function runStepdownDuringRenamePhaseBeforeExplicitEcocCreate(conn, fixture) {
    const client = new EncryptedClient(conn, dbName);

    jsTestLog("Testing stepdown during the rename phase, before explicit ECOC create");
    setupTest(client);
    runCleanupAndStepdownAtFailpoint(conn, fixture, "fleCleanupHangBeforeECOCCreate");

    // On resume, mongos reissues the shardsvrCleanupStructuredEncryptionData command
    // to the new primary. One of two cases may occur:
    // 1. the resumed cleanup is still running, so the new command "joins" it. Since the resumed
    //    cleanup will simply skip the other phases due to change in 'ecoc.compact' existence,
    //    no actual cleanup happens.
    // 2. the resumed cleanup finished, and the reissued command starts a new cleanup, which
    //    actually cleans up the ESC.
    let needManualRestart = true;
    try {
        // check for case 1: reissued cmd joined resumed cleanup
        client.assertStateCollectionsAfterCompact(
            collName, false /* ecocExists */, true /* ecocRenameExists */);
    } catch (error) {
        // case 2: reissued cmd already restarted cleanup
        needManualRestart = false;
    }

    if (needManualRestart) {
        // assert no change to ESC counts
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);

        // running cleanup again should get it to the desired state
        print("Restarting cleanup manually");
        client.runEncryptionOperation(
            () => { assert.commandWorked(client.getDB()[collName].cleanup()); });
    }

    client.assertStateCollectionsAfterCompact(collName, true, false);
    nullAnchorCount += 2;
    anchorCount = 0;
    client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
    client.assertESCNonAnchorCount(collName, nonAnchorCount);
}

// Tests that the coordinator resumes correctly when interrupted by a stepdown
// during the "cleanup" phase, before it deletes non-anchors from ESC.
function runStepdownDuringCleanupPhaseBeforeESCCleanup(conn, fixture) {
    const client = new EncryptedClient(conn, dbName);

    jsTestLog("Testing stepdown during the cleanup phase, before ESC non-anchor cleanup");
    setupTest(client);
    runCleanupAndStepdownAtFailpoint(conn, fixture, "fleCleanupHangBeforeCleanupESCNonAnchors");

    client.assertStateCollectionsAfterCompact(collName, true, false);

    // 2 null anchors inserted and all anchors become unremovable on resume;
    nullAnchorCount += 2;

    // On resume, mongos reissues the shardsvrCleanupStructuredEncryptionData command
    // to the new primary. One of two cases may occur:
    // 1. reissued command joins the ongoing cleanup. No non-anchors are removed.
    // 2. reissued command begins a new cleanup. All non-anchors are removed.
    //
    try {
        // try case 1: no non-anchors removed
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
    } catch (error) {
        // try case 2: all non-anchors removed
        nonAnchorCount = 0;
        client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
    }
    client.assertESCNonAnchorCount(collName, nonAnchorCount);
}

// Tests that the coordinator resumes correctly when interrupted by a stepdown
// during the "anchor deletes" phase.
function runStepdownDuringAnchorDeletesPhase(conn, fixture) {
    const client = new EncryptedClient(conn, dbName);

    jsTestLog("Testing stepdown during the anchor deletes phase");
    setupTest(client);
    runCleanupAndStepdownAtFailpoint(conn, fixture, "fleCleanupHangAfterCleanupESCAnchors");

    client.assertStateCollectionsAfterCompact(collName, true, false);

    // only the null anchors remain since the full cleanup happened before the break
    nullAnchorCount += 2;
    anchorCount = 0;
    nonAnchorCount = 0;
    client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
    client.assertESCNonAnchorCount(collName, nonAnchorCount);
}

// Tests that the coordinator resumes correctly when interrupted during the "drop" phase
function runStepdownDuringDropPhase(conn, fixture) {
    const client = new EncryptedClient(conn, dbName);

    jsTestLog("Testing stepdown during the drop phase");
    setupTest(client);
    runCleanupAndStepdownAtFailpoint(conn, fixture, "fleCleanupHangAfterDropTempCollection");

    client.assertStateCollectionsAfterCompact(collName, true, false);

    // only the null anchors remain since the full cleanup happened before the break
    nullAnchorCount += 2;
    anchorCount = 0;
    nonAnchorCount = 0;
    client.assertEncryptedCollectionCounts(collName, 200, expectedESCCount(), 0);
    client.assertESCNonAnchorCount(collName, nonAnchorCount);
}

{
    const rsOptions = {nodes: 2};
    const st = new ShardingTest({shards: {rs0: rsOptions}, mongos: 1, config: 1});

    st.rs0.nodes.forEach(
        (conn) => { assert.commandWorked(conn.getDB(dbName).setLogLevel(1, "sharding")); });

    runStepdownDuringRenamePhaseBeforeExplicitEcocCreate(st.s, st);
    runStepdownDuringCleanupPhaseBeforeESCCleanup(st.s, st);
    runStepdownDuringAnchorDeletesPhase(st.s, st);
    runStepdownDuringDropPhase(st.s, st);

    st.stop();
}
