/**
 * Tests that a selective restore removes catalog entries of collections not restored and any views
 * created on collections not restored.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */
import {
    getUriForColl,
    getUriForIndex,
    startMongodOnExistingPath
} from "jstests/disk/libs/wt_file_helper.js";
import {backupData} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {IndexBuildTest} from "jstests/noPassthrough/libs/index_build.js";

TestData.skipEnforceFastCountOnValidate = true;

const rst = new ReplSetTest({nodes: 1});

rst.startSet();
rst.initiate();

const primary = rst.getPrimary();

const dbName = "test";
let testDB = primary.getDB(dbName);

assert.commandWorked(testDB.createCollection("a"));
assert.commandWorked(testDB.createCollection("b"));

assert.commandWorked(testDB.createView(/*viewName=*/ "aView", /*viewOn=*/ "a", []));
assert.commandWorked(testDB.createView(/*viewName=*/ "aViewOnAView", /*viewOn=*/ "aView", []));

assert.commandWorked(testDB.createView(/*viewName=*/ "bView", /*viewOn=*/ "b", []));
assert.commandWorked(testDB.createView(/*viewName=*/ "bViewOnAView", /*viewOn=*/ "bView", []));

let collA = testDB.getCollection("a");
assert.commandWorked(collA.insert({x: 1}));

// Start an index build, but don't finish it.
IndexBuildTest.pauseIndexBuilds(primary);
const awaitIndexBuild = IndexBuildTest.startIndexBuild(
    primary, collA.getFullName(), {x: 1}, {}, [ErrorCodes.InterruptedDueToReplStateChange]);
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collA.getName(), /*indexName=*/ "x_1");

// Take a checkpoint.
assert.commandWorked(testDB.adminCommand({fsync: 1}));

// Extract the table names for the collection we won't restore.
const collUri = getUriForColl(collA);
const indexIdUri = getUriForIndex(collA, /*indexName=*/ "_id_");
const indexXUri = getUriForIndex(collA, /*indexName=*/ "x_1");

// Take a backup.
const backupDbpath = primary.dbpath + "/backup";
resetDbpath(backupDbpath);
backupData(primary, backupDbpath);

rst.stopSet(/*signal=*/ null, /*forRestart=*/ true);
awaitIndexBuild();

// Remove the tables for the collection we won't restore.
removeFile(backupDbpath + "/" + collUri + ".wt");
removeFile(backupDbpath + "/" + indexIdUri + ".wt");
removeFile(backupDbpath + "/" + indexXUri + ".wt");

// Start a standalone node in restore mode on the backed up data files.
const mongod = startMongodOnExistingPath(backupDbpath, {restore: ""});

// Startup recovery removes catalog entries for collections not restored.
checkLog.containsJson(mongod, 6260800, {namespace: collA.getFullName(), ident: collUri});

// Startup recovery removes views on collections not restored.
checkLog.containsJson(
    mongod, 6260803, {view: "test.aView", viewOn: "test.a", resolvedNs: "test.a"});
checkLog.containsJson(
    mongod, 6260803, {view: "test.aViewOnAView", viewOn: "test.aView", resolvedNs: "test.a"});

function collectionExists(db, collName) {
    const res = assert.commandWorked(db.runCommand({listCollections: 1, filter: {name: collName}}));
    return res.cursor.firstBatch.length == 1;
}

// Collection "b" and all of its views are restored.
assert(collectionExists(mongod.getDB(dbName), "b"));
assert(collectionExists(mongod.getDB(dbName), "bView"));
assert(collectionExists(mongod.getDB(dbName), "bViewOnAView"));

MongoRunner.stopMongod(mongod);
