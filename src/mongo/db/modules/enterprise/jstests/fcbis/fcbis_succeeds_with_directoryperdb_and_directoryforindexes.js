/**
 * Tests that FCBIS succeeds with directoryperdb and wiredTigerDirectoryForIndexes set.
 * @tags: [requires_persistence, requires_wiredtiger]
 */
function runTest(options) {
    jsTestLog("Running test with " + tojson(options));

    TestData.skipEnforceFastCountOnValidate = true;
    const rst = new ReplSetTest({name: jsTestName(), nodes: [options]});
    rst.startSet();
    rst.initiate();
    const primary = rst.getPrimary();
    const testDB = primary.getDB("test");

    const test2DB = primary.getDB("test2");
    const coll = testDB.coll;
    const coll2 = test2DB.coll2;

    // Create multiple databases and indexes to be cloned.
    assert.commandWorked(coll.insert([{a: 1}, {b: 2}, {c: 3}]));
    assert.commandWorked(coll2.insert([{d: 4}, {e: 5}, {f: 6}, {g: 7}]));

    assert.commandWorked(testDB.runCommand({
        createIndexes: "coll",
        indexes: [{key: {a: 1}, name: 'a_1'}, {key: {b: 1}, name: 'b_1'}],
    }));
    assert.commandWorked(test2DB.runCommand({
        createIndexes: "coll2",
        indexes: [{key: {d: 1}, name: 'd_1'}, {key: {e: 1}, name: 'e_1'}],
    }));

    rst.awaitLastStableRecoveryTimestamp();
    assert.commandWorked(primary.adminCommand({fsync: 1}));

    jsTestLog("Adding the initial sync destination node to the replica set");
    const initialSyncNode = rst.add(Object.assign({
        rsConfig: {priority: 0, votes: 0},
        setParameter: {
            'initialSyncMethod': 'fileCopyBased',
            'numInitialSyncAttempts': 1,
            'logComponentVerbosity': tojson({replication: {verbosity: 1, initialSync: 2}}),
        }
    },
                                                  options));

    jsTestLog("Attempting file copy based initial sync");
    rst.reInitiate();

    rst.awaitSecondaryNodes();
    rst.stopSet();
}

runTest({directoryperdb: "", wiredTigerDirectoryForIndexes: ""});
runTest({directoryperdb: ""});
runTest({wiredTigerDirectoryForIndexes: ""});
runTest({});
