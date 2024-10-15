/**
 * Tests that we can successfully commit a prepared transaction transferred as part of a
 * file copy based initial sync, then continue to commit other transactions.
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 *   uses_prepare_transaction,
 *   uses_transactions,
 * ]
 */

import {PrepareHelpers} from "jstests/core/txns/libs/prepare_helpers.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

TestData.skipEnforceFastCountOnValidate = true;
const replTest = new ReplSetTest({
    nodes: 2,
});
replTest.startSet();

const config = replTest.getReplSetConfig();
replTest.initiate(config);

const primary = replTest.getPrimary();
let secondary = replTest.getSecondary();

// The default WC is majority and this test can't satisfy majority writes.
assert.commandWorked(primary.adminCommand(
    {setDefaultRWConcern: 1, defaultWriteConcern: {w: 1}, writeConcern: {w: "majority"}}));

const dbName = "test";
const collName = TestData.testName;
const testDB = primary.getDB(dbName);
const testColl = testDB.getCollection(collName);

assert.commandWorked(testColl.insert({_id: 1}));

jsTestLog("Preparing a transaction that will be the oldest active transaction");

// Prepare a transaction so that there is an active transaction with an oplog entry. The prepare
// timestamp will become the beginFetchingTimestamp during initial sync.
const session1 = primary.startSession();
const sessionDB1 = session1.getDatabase(dbName);
const sessionColl1 = sessionDB1.getCollection(collName);
session1.startTransaction();
assert.commandWorked(sessionColl1.insert({_id: 2}));
let prepareTimestamp1 = PrepareHelpers.prepareTransaction(session1);

// Add a non-transactional insert.
assert.commandWorked(testColl.insert({_id: 3}));

// We expect the fastcount to reflect the prepared transaction.
assert.eq(2, testColl.find().itcount());
assert.eq(3, testColl.find().count());

// Create a stable checkpoint on the primary node.
replTest.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Restarting the secondary");

// Restart the secondary with startClean set to true so that it goes through initial sync.
// The prepared transaction will be copied as it is part of the stable checkpoint.
replTest.stop(secondary,
              // signal
              undefined,
              // Validation would encounter a prepare conflict on the open transaction.
              {skipValidation: true});
secondary = replTest.start(
    secondary,
    {
        startClean: true,
        setParameter: {
            'logComponentVerbosity': tojson({storage: 1, replication: 4, transaction: 3}),
            'numInitialSyncAttempts': 1,
            'initialSyncMethod': "fileCopyBased"
        }
    },
    true /* wait */);

// Wait for the secondary to complete initial sync.
replTest.awaitSecondaryNodes();

jsTestLog("Initial sync completed");

let secTestColl = secondary.getDB(dbName).getCollection(collName);

// The fast count should reflect the prepared transaction on the initial sync node as well.
assert.eq(2, secTestColl.find().itcount());
assert.eq(3, secTestColl.find().count());

// Commit the transaction now that the new secondary is available.
assert.commandWorked(PrepareHelpers.commitTransaction(session1, prepareTimestamp1));
replTest.awaitReplication();

jsTestLog("T1 committed");

// Make sure the transaction committed properly and is reflected after the initial sync.
let res = secTestColl.findOne({_id: 2});
assert.docEq(res, {_id: 2}, res);

// Step up the secondary after initial sync is done and make sure we can successfully run
// another transaction.
replTest.stepUp(secondary);
replTest.waitForState(secondary, ReplSetTest.State.PRIMARY);
let newPrimary = replTest.getPrimary();
const session2 = newPrimary.startSession();
const sessionDB2 = session2.getDatabase(dbName);
const sessionColl2 = sessionDB2.getCollection(collName);
jsTestLog("T2 starting");
session2.startTransaction();
assert.commandWorked(sessionColl2.insert({_id: 4}));
jsTestLog("T2 first command executed, preparing");
let prepareTimestamp2 = PrepareHelpers.prepareTransaction(session2);
jsTestLog("T2 prepared");
assert.commandWorked(PrepareHelpers.commitTransaction(session2, prepareTimestamp2));
jsTestLog("T2 committed");
res = newPrimary.getDB(dbName).getCollection(collName).findOne({_id: 4});
assert.docEq(res, {_id: 4}, res);

replTest.stopSet();
