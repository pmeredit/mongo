/**
 * Concurrency workload to test importing collections on a live replica set.
 *
 *  @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js");

const dbName = "crud";

function exportCollections(numCollections) {
    let standalone = MongoRunner.runMongod({setParameter: "featureFlagLiveImportExport=true"});
    let db = standalone.getDB(dbName);

    jsTest.log("Creating " + numCollections + " collections");
    for (let i = 0; i < numCollections; i++) {
        const collName = "importCollection-" + i;
        assert.commandWorked(db.createCollection(collName));

        const coll = db.getCollection(collName);
        const numIndexesToBuild = Number.parseInt(Math.random() * 5);
        for (let j = 0; j < numIndexesToBuild; j++) {
            let key = Object.create({});
            key[j.toString()] = 1;
            assert.commandWorked(coll.createIndex(key));
        }
    }

    MongoRunner.stopMongod(standalone);

    jsTest.log("Exporting " + numCollections + " collections");
    standalone = MongoRunner.runMongod({
        setParameter: "featureFlagLiveImportExport=true",
        dbpath: standalone.dbpath,
        noCleanData: true,
        queryableBackupMode: ""
    });

    db = standalone.getDB(dbName);

    let exportedCollections = [];
    for (let i = 0; i < numCollections; i++) {
        const collName = "importCollection-" + i;
        exportedCollections.push(assert.commandWorked(db.runCommand({exportCollection: collName})));
    }

    MongoRunner.stopMongod(standalone);
    return exportedCollections;
}

function importRandomCollection(primaryDB, nodes, collName, collectionProperties) {
    // Copy the exported files into the path of each replica set node.
    nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

    // Import and validate the collection on the replica set.
    assert.commandWorked(primaryDB.runCommand({
        importCollection: collName,
        collectionProperties: collectionProperties,
        force: Math.random() >= 0.5,
        writeConcern: {w: 3}
    }));

    validateImportCollection(primaryDB.getCollection(collName), collectionProperties);
}

/**
 * Starts a client that will run a FSM workload.
 */
function fsmClient(host) {
    // Launch FSM client.
    const suite = 'concurrency_replication_for_export_import';
    const resmokeCmd = 'python buildscripts/resmoke.py run --shuffle --continueOnFailure' +
        ' --repeat=99999 --internalParam=is_inner_level --mongo=' + MongoRunner.mongoShellPath +
        ' --shellConnString=mongodb://' + host + ' --suites=' + suite;

    // Returns the pid of the FSM test client so it can be terminated without waiting for its
    // execution to finish.
    return _startMongoProgram({args: resmokeCmd.split(' ')});
}

let numCollections = 500;
let collectionsToImport = exportCollections(numCollections);

jsTestLog("Starting a replica set for import");
const rst = new ReplSetTest({nodes: 3});
const nodes = rst.startSet({setParameter: "featureFlagLiveImportExport=true"});
rst.initiateWithHighElectionTimeout();
const primary = rst.getPrimary();
const primaryDB = primary.getDB(dbName);

// Start running FSM workloads against the primary.
const fsmPid = fsmClient(primary.host);

for (let i = 0; i < numCollections; i++) {
    importRandomCollection(primaryDB, nodes, "importCollection-" + i, collectionsToImport[i]);
}

const fsmStatus = checkProgram(fsmPid);
assert(fsmStatus.alive,
       jsTest.name() + ' FSM client was not running at end of test and exited with code: ' +
           fsmStatus.exitCode);

const kSIGINT = 2;
const exitCode = stopMongoProgramByPid(fsmPid, kSIGINT);
if (!_isWindows()) {
    // The mongo shell calls TerminateProcess() on Windows rather than more gracefully
    // interrupting resmoke.py test execution.
    assert.eq(130, exitCode, 'expected resmoke.py to exit due to being interrupted');
}

rst.stopSet();
}());
