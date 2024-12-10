/**
 * FCBIS copies pre-images over from the sync source - whereas logical initial sync doesn't sync
 * pre-images to the destination. Thus, FCBIS can interrupt / impact pre-image truncate marker
 * initialization and truncation.
 *
 * Tests restarting file copy based initial sync at different points when pre-images are present on
 * the sync source. Ensures pre image truncate marker initialization and truncation handle
 * pre-populated pre-images collections and FCBIS restarts.
 * @tags: [requires_persistence, requires_wiredtiger]
 */
import {assertDropAndRecreateCollection} from "jstests/libs/collection_drop_recreate.js";
import {kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";
import {
    getPreImagesCollection,
} from "jstests/libs/query/change_stream_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

TestData.skipEnforceFastCountOnValidate = true;
const rst = new ReplSetTest({
    nodes: [{}, {rsConfig: {priority: 0}}],
});
const expiredChangeStreamPreImageRemovalJobSleepSecs = 1;
rst.startSet({setParameter: {expiredChangeStreamPreImageRemovalJobSleepSecs}});
rst.initiate();

let primary = rst.getPrimary();
let primaryDB = primary.getDB("testDB");
const collNameA = "testCollA";
const collNameB = "testCollB";
let primaryCollA = assertDropAndRecreateCollection(
    primaryDB, collNameA, {changeStreamPreAndPostImages: {enabled: true}});
const primaryCollB = assertDropAndRecreateCollection(
    primaryDB, collNameB, {changeStreamPreAndPostImages: {enabled: true}});

const originalDocs = [
    {a: 1},
    {a: 2},
];

function waitForFailpoint(initialSyncNode, failpoint) {
    assert.soonNoExcept(() => {
        let res = initialSyncNode.adminCommand({
            waitForFailPoint: failpoint,
            timesEntered: 1,
            maxTimeMS: kDefaultWaitForFailPointTimeout
        });

        return res.ok;
    });
}

function createInititialSyncNodeAndWaitForFailpoint(initialSyncMethod, failpoint, data = {}) {
    const failpointStr = `failpoint.${failpoint}`;
    let initialSyncNode = rst.add({
        rsConfig: {priority: 0},
        setParameter: {
            initialSyncMethod,
            expiredChangeStreamPreImageRemovalJobSleepSecs,
            [[failpointStr]]: tojson({mode: 'alwaysOn', data}),
            'failpoint.forceSyncSourceCandidate':
                tojson({mode: 'alwaysOn', data: {hostAndPort: primary.name}}),
            'logComponentVerbosity': tojson({replication: {verbosity: 2}, storage: {verbosity: 2}}),
        }
    });
    rst.reInitiate();
    jsTestLog(`Waiting for initial sync node to reach ${failpoint}`);
    waitForFailpoint(initialSyncNode, failpoint);
    return initialSyncNode;
}

function restartAndWaitToFinishSync(initialSyncMethod, initialSyncNode, startClean = false) {
    rst.restart(initialSyncNode, {
        startClean,
        setParameter: {
            initialSyncMethod,
            expiredChangeStreamPreImageRemovalJobSleepSecs,
            'failpoint.forceSyncSourceCandidate':
                tojson({mode: 'alwaysOn', data: {hostAndPort: primary.name}}),
            'logComponentVerbosity': tojson(
                {replication: {verbosity: 2}, storage: {verbosity: 2}, command: {verbosity: 2}}),
        }
    });

    rst.awaitSecondaryNodes(null, [initialSyncNode]);
}

function restartAndWaitForFailpoint(
    initialSyncMethod, initialSyncNode, failpoint, startClean = false) {
    const failpointStr = `failpoint.${failpoint}`;
    rst.restart(initialSyncNode, {
        startClean,
        setParameter: {
            initialSyncMethod,
            expiredChangeStreamPreImageRemovalJobSleepSecs,
            [[failpointStr]]: tojson({mode: 'alwaysOn'}),
            'failpoint.forceSyncSourceCandidate':
                tojson({mode: 'alwaysOn', data: {hostAndPort: primary.name}}),
            'logComponentVerbosity': tojson({replication: {verbosity: 1}, storage: {verbosity: 2}}),

        }
    });

    jsTestLog(`Waiting for initial sync node to reach ${failpoint}`);
    waitForFailpoint(initialSyncNode, failpoint);
}

// Get's pre-images in their $natural order. First ordered by nsUUID, then their 'ts' field.
function getPreImages(node) {
    return getPreImagesCollection(node).find().hint({$natural: 1}).allowDiskUse().toArray();
}

// Generates a set of pre-images from 'coll'. When round = 0, resulting pre-images are the original
// documents for the collection. For round > 0, generates pre-images for 'preImageRound': round.
function generateRoundOfPreImages(coll, round) {
    jsTest.log(`Generating set of pre-images for ${coll.getFullName()}, round ${round}`);
    if (round == 0) {
        // Insert the base set of documents so the updates generate pre-images. Append the
        // 'collName' field to make debugging pre-images for each collection easier.
        const originalDocsForColl =
            originalDocs.map((doc) => { return {...doc, collName: coll.getFullName()}; });
        assert.commandWorked(coll.insert(originalDocsForColl));
    }

    // Prepare documents to be pre-images for the next 'round'.
    assert.commandWorked(coll.update({}, {$set: {'preImageRound': round + 1}}, {multi: true}));
}

// Tests FCBIS restart when pre-images exist / are generated for 2 different pre-image enabled
// collections at different points across inital sync restarts.
function runFCBISTest() {
    const initialSyncMethod = 'fileCopyBased';
    assert.commandWorked(primary.getDB('admin').runCommand({
        setClusterParameter: {changeStreamOptions: {preAndPostImages: {expireAfterSeconds: 'off'}}}
    }));

    // First round of initial sync begins with pre-images for only 'primaryCollA'.
    generateRoundOfPreImages(primaryCollA, 0);
    rst.awaitReplication();
    rst.checkPreImageCollection();

    // Ensure there's an up-to-date stable checkpoint for FCBIS to copy.
    rst.awaitLastStableRecoveryTimestamp();
    assert.commandWorked(primary.adminCommand({fsync: 1}));

    jsTestLog(
        'Creating initial sync node and hanging initial sync once it starts file cloning from the sync source');
    let initialSyncNode = createInititialSyncNodeAndWaitForFailpoint(
        initialSyncMethod, 'fCBISHangAfterStartingFileClone');

    // Create pre-images for 'primaryCollB' after cloning from the sync source starts.
    generateRoundOfPreImages(primaryCollB, 0);

    jsTestLog('Restarting node and hanging after initial sync finishes deleting old storage files');
    restartAndWaitForFailpoint(
        initialSyncMethod, initialSyncNode, 'fCBISHangAfterDeletingOldStorageFiles');

    // Before completing the first initial sync, insert the second set of pre-images for both
    // collections.
    generateRoundOfPreImages(primaryCollA, 1);
    generateRoundOfPreImages(primaryCollB, 1);

    jsTestLog('Completing first initial sync');
    restartAndWaitToFinishSync(initialSyncMethod, initialSyncNode);

    // The node is done with its first initial sync. Generate additional pre-images to ensure
    // there aren't 'holes' between the pre-images written before, during, and after the first
    // inital sync completes.
    generateRoundOfPreImages(primaryCollA, 2);
    generateRoundOfPreImages(primaryCollB, 2);

    rst.checkPreImageCollection();

    // Ensure there is a stable checkpoint so the second initial sync starts with pre-images for
    // both collections.
    rst.awaitLastStableRecoveryTimestamp();
    assert.commandWorked(primary.adminCommand({fsync: 1}));

    // Restart the replica set, including the primary. This ensures the primary actually kills
    // the backup cursor from the last FCBIS attempt.
    // Since this is an intermediate restart, skip consistency checks to make stopSet faster.
    jsTestLog(
        'Restarting set to ensure primary has killed backup cursor from previous FCBIS attempt');
    rst.stopSet(undefined /* signal */,
                true /* forRestart */,
                {skipCheckDBHashes: true, skipValidation: true});
    rst.startSet({setParameter: {expiredChangeStreamPreImageRemovalJobSleepSecs}},
                 true /* forRestart */);
    primary = rst.getPrimary();
    primaryDB = primary.getDB("testDB");
    primaryCollA = primaryDB.getCollection(collNameA);
    // Ensure data exists from before the restart.
    assert.neq(primaryCollA.find().count(), 0);

    jsTestLog(
        `Restarting node cleanly to test second initial sync. Hanging intial sync after it finishes moving new files from the '.initalsync' directory to the original dbpath`);
    restartAndWaitForFailpoint(
        initialSyncMethod,
        initialSyncNode,
        'fCBISHangAfterMovingTheNewFiles',
        true,
    );

    generateRoundOfPreImages(primaryCollA, 3);

    jsTestLog(`Restarting node to finish second initial sync`);
    restartAndWaitToFinishSync(initialSyncMethod, initialSyncNode);
    rst.checkPreImageCollection();

    jsTest.log(`Pre-images after second intial sync. Primary ${
        tojson(
            getPreImages(primary))}, initial sync node ${tojson(getPreImages(initialSyncNode))}`);

    // Set expireAfterSeconds and ensure all pre-images are expired.
    assert.commandWorked(primary.getDB('admin').runCommand(
        {setClusterParameter: {changeStreamOptions: {preAndPostImages: {expireAfterSeconds: 1}}}}));

    // Ensure purging job deletes the expired pre-image entries of the test collection.
    rst.nodes.forEach((node) => {
        assert.soon(() => { return getPreImages(node).length == 0; },
                    `Unexpected number of pre-images on node ${node.port}. Found pre-images ${
                        tojson(getPreImages(node))}`);
    });
}

runFCBISTest();
rst.stopSet();
