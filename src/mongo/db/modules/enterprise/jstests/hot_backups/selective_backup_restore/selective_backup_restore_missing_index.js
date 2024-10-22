/**
 * Tests that a selective restore fails when restoring a collection with missing index files.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */
import {getUriForIndex, startMongodOnExistingPath} from "jstests/disk/libs/wt_file_helper.js";
import {backupData} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

TestData.skipEnforceFastCountOnValidate = true;

const rst = new ReplSetTest({nodes: 1});

rst.startSet();
rst.initiate();

const primary = rst.getPrimary();

const dbName = "test";
let testDB = primary.getDB(dbName);

assert.commandWorked(testDB.createCollection("a"));

let coll = testDB.getCollection("a");
assert.commandWorked(coll.createIndex({x: 1}));

// Take a checkpoint.
assert.commandWorked(testDB.adminCommand({fsync: 1}));

// Extract the table name for index {x: 1}.
const indexUri = getUriForIndex(coll, /*indexName=*/ "x_1");

// Take a backup.
const backupDbpath = primary.dbpath + "/backup";
resetDbpath(backupDbpath);
backupData(primary, backupDbpath);

rst.stopSet(/*signal=*/ null, /*forRestart=*/ true);

// Remove the table for index {x: 1}.
removeFile(backupDbpath + "/" + indexUri + ".wt");

// Trying to restore collection "a" with the data files for index {x: 1} missing will crash.
assert.throws(() => { startMongodOnExistingPath(backupDbpath, {restore: ""}); });

assert.gte(rawMongoProgramOutput("Fatal assertion").search("6261000"), 0);
