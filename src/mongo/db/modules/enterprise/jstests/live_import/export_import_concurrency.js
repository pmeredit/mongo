/**
 * Concurrency workload to test importing collections on a live replica set.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
import {funWithArgs} from "jstests/libs/parallel_shell_helpers.js";
import {getPython3Binary} from "jstests/libs/python.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

const dbName = "crud";

function exportCollections(numImportThreads, numImportsPerThread) {
    const rst = new ReplSetTest({nodes: 1, name: "export_collection_target"});
    const nodes = rst.startSet();
    rst.initiate();
    let primary = rst.getPrimary();
    let db = primary.getDB(dbName);

    let collections = [];
    jsTestLog("Creating " + (numImportThreads * numImportsPerThread) + " collections");
    for (let tid = 0; tid < numImportThreads; tid++) {
        for (let i = 0; i < numImportsPerThread; i++) {
            const collName = `importCollection-t${tid}-${i}`;
            assert.commandWorked(db.createCollection(collName));

            const coll = db.getCollection(collName);
            const numIndexesToBuild = Number.parseInt(Math.random() * 5);
            for (let j = 0; j < numIndexesToBuild; j++) {
                let key = Object.create({});
                key[j.toString()] = 1;
                assert.commandWorked(coll.createIndex(key));
            }
            collections.push({tid: tid, name: collName});
        }
    }

    rst.stopSet(null, false, {noCleanData: true});

    jsTestLog("Exporting" + (numImportThreads * numImportsPerThread) + " collections");
    let standalone = MongoRunner.runMongod({
        dbpath: rst.getDbPath(nodes[0]),
        noCleanData: true,
        queryableBackupMode: "",
        setParameter: {wiredTigerSkipTableLoggingChecksOnStartup: true}
    });

    db = standalone.getDB(dbName);

    collections.forEach((coll) => {
        coll.collectionProperties =
            assert.commandWorked(db.runCommand({exportCollection: coll.name}));
    });

    MongoRunner.stopMongod(standalone, null, {skipValidation: true});
    return collections;
}

/**
 * Starts a client that will run a FSM workload.
 */
function fsmClient(host) {
    // Launch FSM client.
    const suite = 'concurrency_replication_for_export_import';
    const resmokeCmd = getPython3Binary() +
        ' buildscripts/resmoke.py run --shuffle --continueOnFailure' +
        ' --repeat=99999 --internalParam=is_inner_level --mongo=' +
        MongoRunner.getMongoShellPath() + ' --shellConnString=mongodb://' + host +
        ' --suites=' + suite;

    // Returns the pid of the FSM test client so it can be terminated without waiting for its
    // execution to finish.
    return _startMongoProgram({args: resmokeCmd.split(' ')});
}

const numImportThreads = 4;
const numImportsPerThread = 50;
// This returns an array of {tid, name, collectionProperties};
const collectionsToImport = exportCollections(numImportThreads, numImportsPerThread);

jsTestLog("Starting a replica set for import");
const rst = new ReplSetTest({nodes: 3});
const nodes = rst.startSet();
rst.initiate();
const primary = rst.getPrimary();

// Insert the 'collectionsToImport' metadata to the database to avoid getting "Argument list too
// long" error when passing a large array to parallel shells as an argument.
assert.commandWorked(primary.getDB(dbName).runCommand({
    insert: "collectionsToImport",
    documents: collectionsToImport,
}));

// Start running FSM workloads against the primary.
let fsmPid = 0;
let attempts = 0;

// Make sure the fsm client does not fail within the first few seconds of running due to some
// unrelated network error (usually when fetching yml files from a remote location).
do {
    jsTestLog("Attempt " + attempts + " of starting up the fsm client");
    fsmPid = fsmClient(primary.host);
    attempts += 1;
    sleep(5 * 1000);
} while (!checkProgram(fsmPid).alive && attempts < 10);

const importFn = async function(dbName, tid, dbPaths) {
    const {copyFilesForExport, validateImportCollection} = await import(
        "src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js");

    const testDB = db.getMongo().getDB(dbName);

    if (_isWindows()) {
        // Correct double-escaping of '\' when passing dbPaths through the parallel shell.
        dbPaths = dbPaths.map((dbPath) => dbPath.replace(/\\/g, "\\"));
    }

    function importCollection(collName, collectionProperties) {
        jsTestLog("Thread " + tid + ": importing " + collName);
        // Copy the exported files into the path of each replica set node.
        dbPaths.forEach((dbPath) => copyFilesForExport(collectionProperties, dbPath));

        // Import and validate the collection on the replica set.
        assert.commandWorked(testDB.runCommand({
            importCollection: collectionProperties,
            force: Math.random() >= 0.5,
            writeConcern: {w: 3}
        }));

        validateImportCollection(testDB.getCollection(collName), collectionProperties);
    }

    // Get the collections to import for this thread.
    const collectionsToImport =
        testDB.getCollection("collectionsToImport").find({tid: tid}).sort({name: 1}).toArray();
    collectionsToImport.forEach((coll) => importCollection(coll.name, coll.collectionProperties));
};

jsTestLog("Starting " + numImportThreads + " import threads");
let importThreads = [];
for (let tid = 0; tid < numImportThreads; tid++) {
    importThreads.push(startParallelShell(
        funWithArgs(importFn, dbName, tid, nodes.map((node) => rst.getDbPath(node))),
        primary.port));
}

importThreads.forEach((waitForThread, tid) => {
    jsTestLog("Joining thread " + tid);
    waitForThread();
});
jsTestLog("Finished importing collections");

const fsmStatus = checkProgram(fsmPid);
// If the fsmClient ran successfully then kill it, otherwise log that it was not running and move
// on. This is okay since the fsm client would have failed for reasons unrelated to the restore
// procedure.
if (fsmStatus.alive) {
    const kSIGINT = 2;
    const exitCode = stopMongoProgramByPid(fsmPid, kSIGINT);
    if (!_isWindows()) {
        // The mongo shell calls TerminateProcess() on Windows rather than more gracefully
        // interrupting resmoke.py test execution.

        // resmoke.py may exit cleanly on SIGINT, returning 130 if the suite tests were running and
        // returning SIGINT otherwise. It may also exit uncleanly, in which case
        // stopMongoProgramByPid returns -SIGINT. See SERVER-67390 and SERVER-72449.
        assert(exitCode == 130 || exitCode == -kSIGINT || exitCode == kSIGINT,
               'expected resmoke.py to exit due to being interrupted, but exited with code: ' +
                   exitCode);
    }
} else {
    jsTestLog(jsTest.name() + ' FSM client was not running at end of test and exited with code: ' +
              fsmStatus.exitCode);
}

rst.stopSet();
