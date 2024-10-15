/**
 * Test for if a node is in an extension round of File Copy Based Initial Sync and its sync source
 * is ahead of the majority commit point, then the new node will return from the extension round,
 * complete the sync and become a secondary (instead of timing out and failing initial sync).
 * @tags: [requires_persistence, requires_wiredtiger]
 */
import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {forceSyncSource} from "jstests/replsets/libs/sync_source.js";
// Create replset with 3 nodes.
const dbName = "testdb";
const colName = "testcoll";

const replset = new ReplSetTest({name: jsTestName(), nodes: [{}, {rsConfig: {priority: 0}}]});

replset.startSet();
replset.initiate();

const primary = replset.getPrimary();
const secondary = replset.getSecondaries()[0];

const primaryDb = primary.getDB(dbName);
const primaryColl = primaryDb.getCollection(colName);

assert.commandWorked(primaryColl.insert({x: 1, y: 2}));
replset.awaitReplication();

jsTestLog("Primary and secondary node setup complete.");

// Create new node to be added using File Copy Based Initial Sync, with primary as sync
// source.
const initialSyncNodeConfig = {
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'initialSyncSourceReadPreference': 'primary',
        'fileBasedInitialSyncExtendCursorTimeoutMS': 1000,
        'failpoint.forceSyncSourceCandidate':
            tojson({mode: "alwaysOn", data: {hostAndPort: primary.name}}),
        'failpoint.fCBISForceExtendBackupCursor': tojson({mode: "alwaysOn"}),
        "numInitialSyncAttempts": 1,
    }
};

// Stop replication on secondary so that the initial sync source can replicate
// a write and be ahead of the majority commit point.
const stopReplProducerFailPoint = configureFailPoint(secondary, 'stopReplProducer');

stopReplProducerFailPoint.wait();

// Replicate a write on just the primary.
assert.commandWorked(primaryColl.insert({x: 4, y: 5}, {writeConcern: {w: 1}}));

jsTestLog("Write replicated to primary only.");

// Assert that the last applied time on the sync source is ahread of the majority commit
// point.
const status = assert.commandWorked(primary.adminCommand({replSetGetStatus: 1}));
const appliedTimestamp = status.optimes.appliedOpTime.ts;
const committedTimestamp = status.optimes.lastCommittedOpTime.ts;
assert(timestampCmp(committedTimestamp, appliedTimestamp) == -1);

jsTestLog("Sync source has last applied ahead of majority commit point.");

// Add the initialSyncNode to the set.
const initialSyncNode = replset.add(initialSyncNodeConfig);
jsTestLog("Added initialSyncNode.");
replset.reInitiate();

replset.waitForState(initialSyncNode, ReplSetTest.State.STARTUP_2);

// Check that the correct warning was printed when it ends the extension round.
checkLog.checkContainsOnceJson(initialSyncNode, 7929800, {});

// Wait for the new node to complete FCBIS and become a secondary.
replset.waitForState(initialSyncNode, ReplSetTest.State.SECONDARY);

stopReplProducerFailPoint.off();

replset.stopSet();
