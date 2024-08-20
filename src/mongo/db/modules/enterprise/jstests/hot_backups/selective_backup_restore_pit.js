/**
 * Selective Backup & PIT Restore
 *
 * This test exercises selective restore on operations and transactions
 * that span both restored & non-restored collections in the same database.
 * Operations on non-restored collections should be skipped while any operation,
 * even within a multi-document multi-collection transaction, should still apply
 * to the selectively restored collection.
 *
 * Note that the procedure for selective restore requires applying
 * oplog entries to each node in a replicaSet as standalone.
 *
 *
 * @tags: [
 *     requires_persistence,
 *     requires_replication,
 *     requires_wiredtiger
 * ]
 */
import {PrepareHelpers} from "jstests/core/txns/libs/prepare_helpers.js";
import {_copyFileHelper, openBackupCursor} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {IndexBuildTest} from "jstests/noPassthrough/libs/index_build.js";

TestData.skipEnforceFastCountOnValidate = true;

function generateData(db, collectionName) {
    assert.commandWorked(db.createCollection(collectionName));

    for (let i = 0; i < 5; i++) {
        assert.commandWorked(db.getCollection(collectionName).insert({x: i}));
    }
}

function generateOplogEntries(db, dbName, collectionNames) {
    for (const collName of collectionNames) {
        const coll = db.getCollection(collName);

        // Perform CRUD ops.
        assert.commandWorked(coll.insert({x: 5}));
        assert.commandWorked(coll.remove({x: 0}));
        assert.commandWorked(coll.update({x: 1}, {x: 10}));

        // Run collMod.
        assert.commandWorked(db.runCommand({collMod: collName, validator: {x: {$gte: 0}}}));

        // Start single-phase index builds and finish them.
        assert.commandWorked(coll.createIndex({y: 1}));

        // Run a transaction within the collection.
        const session = db.getMongo().startSession();
        const sessionDb = session.getDatabase(dbName);
        const sessionColl = sessionDb[collName];
        session.startTransaction();
        assert.commandWorked(sessionColl.insert({x: 100}));
        assert.commandWorked(sessionColl.insert({x: 101}));
        assert.commandWorked(session.commitTransaction_forTesting());

        // Run a prepared transaction within the collection.
        session.startTransaction();
        assert.commandWorked(sessionColl.insert({x: 200}));
        assert.commandWorked(sessionColl.insert({x: 201}));
        const prepareTimestamp = PrepareHelpers.prepareTransaction(session);
        assert.commandWorked(PrepareHelpers.commitTransaction(session, prepareTimestamp));
    }

    // Run a transaction across both collections.
    const session = db.getMongo().startSession();
    const sessionDb = session.getDatabase(dbName);
    session.startTransaction();
    assert.commandWorked(sessionDb.getCollection("a").insert({x: 300}));
    assert.commandWorked(sessionDb.getCollection("b").insert({x: 300}));
    assert.commandWorked(sessionDb.getCollection("a").insert({x: 301}));
    assert.commandWorked(sessionDb.getCollection("b").insert({x: 301}));
    assert.commandWorked(session.commitTransaction_forTesting());

    // Run a prepared transaction spanning both collections.
    session.startTransaction();
    assert.commandWorked(sessionDb.getCollection("a").insert({x: 400}));
    assert.commandWorked(sessionDb.getCollection("b").insert({x: 400}));
    assert.commandWorked(sessionDb.getCollection("a").insert({x: 401}));
    assert.commandWorked(sessionDb.getCollection("b").insert({x: 401}));
    let prepareTimestamp = PrepareHelpers.prepareTransaction(session);
    assert.commandWorked(PrepareHelpers.commitTransaction(session, prepareTimestamp));

    // Rename "b" -> "c".
    assert.commandWorked(db.adminCommand({renameCollection: "test.b", to: "test.c"}));

    IndexBuildTest.pauseIndexBuilds(primary);

    return session;
}

function validateOplogEntries(coll, numOps) {
    assert.eq(numOps, coll.find({}).itcount());

    // CRUD ops.
    assert.eq(1, coll.find({x: 10}).itcount());
    assert.eq(1, coll.find({x: 2}).itcount());
    assert.eq(1, coll.find({x: 3}).itcount());
    assert.eq(1, coll.find({x: 4}).itcount());
    assert.eq(1, coll.find({x: 5}).itcount());

    // From multi-doc transaction within collection.
    assert.eq(1, coll.find({x: 100}).itcount());
    assert.eq(1, coll.find({x: 101}).itcount());

    // From multi-doc prep transaction within collection.
    assert.eq(1, coll.find({x: 200}).itcount());
    assert.eq(1, coll.find({x: 201}).itcount());

    // From multi-doc transaction across both collections.
    assert.eq(1, coll.find({x: 300}).itcount());
    assert.eq(1, coll.find({x: 301}).itcount());

    // From multi-doc prep transaction across both collections.
    assert.eq(1, coll.find({x: 400}).itcount());
    assert.eq(1, coll.find({x: 401}).itcount());
}

function validateIndexes(db, isIndexBuildDone) {
    let listIndexesRes =
        assert.commandWorked(db.runCommand({listIndexes: "a", includeIndexBuildInfo: true}))
            .cursor.firstBatch;
    jsTestLog(listIndexesRes);
    assert.eq(3, listIndexesRes.length);
    assert.eq("_id_", listIndexesRes[0].spec.name);
    assert(!listIndexesRes[0].hasOwnProperty("indexBuildInfo"));

    assert.eq("y_1", listIndexesRes[1].spec.name);
    assert(!listIndexesRes[1].hasOwnProperty("indexBuildInfo"));

    assert.eq("x_1", listIndexesRes[2].spec.name);
    assert.neq(isIndexBuildDone, listIndexesRes[2].hasOwnProperty("indexBuildInfo"));
}

function validateCollectionsForTest(db) {
    let listCollsRes = assert.commandWorked(db.runCommand({listCollections: 1})).cursor.firstBatch;
    jsTestLog(listCollsRes);
    assert.eq(1, listCollsRes.length);
    assert.eq("a", listCollsRes[0].name);
    assert.eq({x: {$gte: 0}}, listCollsRes[0].options.validator);
}

function validateSelectiveBackupRestore(db, dbName, backupDbPath) {
    conn = MongoRunner.runMongod({
        dbpath: backupDbPath,
        noCleanData: true,
        restore: "",
        setParameter: {recoverFromOplogAsStandalone: true, takeUnstableCheckpointOnShutdown: true}
    });
    assert(conn);
    db = conn.getDB(dbName);

    // Ensure that only "a" exists. Check that the state of "a" is expected after oplog recovery.
    validateCollectionsForTest(db);

    // Has index build info as it's still not built.
    validateIndexes(db, /*isIndexBuildDone=*/ false);

    validateOplogEntries(db.getCollection("a"), /*numOps=*/ 13);
    MongoRunner.stopMongod(conn);
}

//
// main
//
let rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiateWithHighElectionTimeout();

let primary = rst.getPrimary();
const dbName = "test";
let db = primary.getDB(dbName);

let collectionNames = ["a", "b"];

// Opening backup cursors can race with taking a checkpoint, so disable checkpoints.
// This makes testing quicker and more predictable. In production, a poorly interleaved checkpoint
// will return an error, requiring retry.
assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseCheckpointThread", mode: "alwaysOn"}));

// Create two collections with some initial data.
generateData(db, collectionNames[0]);
generateData(db, collectionNames[1]);

// Take the checkpoint to be used by the backup cursor.
assert.commandWorked(db.adminCommand({fsync: 1}));

// Perform actions on collections after the checkpoint.
let session = generateOplogEntries(db, dbName, collectionNames);

// Start two-phase index builds without finishing them.
const awaitIndexBuildA = IndexBuildTest.startIndexBuild(primary, "test.a", {x: 1});
IndexBuildTest.waitForIndexBuildToScanCollection(db, "a", "x_1");

const awaitIndexBuildC = IndexBuildTest.startIndexBuild(primary, "test.c", {x: 1});
IndexBuildTest.waitForIndexBuildToScanCollection(db, "c", "x_1");

// Run a prepared transaction spanning both collections WITHOUT committing.
const sessionDb = session.getDatabase(dbName);
session.startTransaction();
assert.commandWorked(sessionDb.getCollection("a").insert({x: 500}));
assert.commandWorked(sessionDb.getCollection("c").insert({x: 500}));
assert.commandWorked(sessionDb.getCollection("a").insert({x: 501}));
assert.commandWorked(sessionDb.getCollection("c").insert({x: 501}));
let prepareTimestamp = PrepareHelpers.prepareTransaction(session);

const lsid = session.getSessionId();
const txnNumber = session.getTxnNumber_forTesting();

const backupDbPath = primary.dbpath + "/backup";
resetDbpath(backupDbPath);
mkdir(backupDbPath + "/journal");

// Open a backup cursor on the checkpoint.
let backupCursor = openBackupCursor(primary.getDB("admin"));

// Print the metadata document.
assert(backupCursor.hasNext());
jsTestLog(backupCursor.next());

while (backupCursor.hasNext()) {
    let doc = backupCursor.next();

    // Copy everything but collection "test.b" and its indexes.
    if (doc.ns == "test.b") {
        jsTestLog("Skipping for backup: " + tojson(doc));
        continue;
    }

    jsTestLog("Copying for backup: " + tojson(doc));
    _copyFileHelper({filename: doc.filename, fileSize: doc.fileSize}, primary.dbpath, backupDbPath);
}

backupCursor.close();

// Abort the prepared transaction.
assert.commandWorked(session.abortTransaction_forTesting());

// Finish two-phase index builds.
IndexBuildTest.resumeIndexBuilds(primary);
awaitIndexBuildA();
awaitIndexBuildC();

assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseCheckpointThread", mode: "off"}));

rst.stopSet(/*signal=*/ null, /*forRestart=*/ true);

// Startup on the backed up data files to clean up the catalog.
let conn = MongoRunner.runMongod({dbpath: backupDbPath, noCleanData: true, restore: ""});
assert(conn);

// Remove the previous replica set configuration and other collections.
const localDb = conn.getDB("local");
const replConfig = localDb.system.replset.findOne();
assert.commandWorked(localDb.system.replset.remove({}));

localDb.replset.oplogTruncateAfterPoint.drop();
localDb.replset.election.drop();
localDb.replset.initialSyncId.drop();

// Create a new replica set configuration.
replConfig.members = [{_id: NumberInt(0), host: "localhost:" + primary.port}];
replConfig.protocolVersion = 1;
assert.commandWorked(localDb.system.replset.insert(replConfig));

MongoRunner.stopMongod(conn);

// Startup again but with 'recoverFromOplogAsStandalone: true' this time.
validateSelectiveBackupRestore(db, dbName, backupDbPath);

clearRawMongoProgramOutput();

// Finally, restart the node as a replica set to build any unfinished index builds.
rst = new ReplSetTest({
    nodes: [{dbpath: backupDbPath, noCleanData: true, port: primary.port}],
    nodeOptions: {setParameter: {logComponentVerbosity: tojsononeline({storage: {recovery: 2}})}}
});
rst.startSet();

primary = rst.getPrimary();
db = primary.getDB(dbName);

// Committing a prepared transaction cannot be run before its prepare oplog entry has been majority
// committed. Wait for a stable checkpoint to be performed.
checkLog.containsJson(primary, 23986);

// Commit the un-committed prepared transaction.
const commitSession = PrepareHelpers.createSessionWithGivenId(primary, lsid);
const commitSessionDB = commitSession.getDatabase(dbName);
commitSession.setTxnNumber_forTesting(txnNumber);
assert.commandWorked(commitSessionDB.adminCommand({
    commitTransaction: 1,
    commitTimestamp: prepareTimestamp,
    txnNumber: NumberLong(txnNumber),
    autocommit: false,
}));

// Wait for "x_1" to finish building before doing checks.
assert.soonNoExcept(() => {
    IndexBuildTest.assertIndexes(
        db.getCollection("a"), 3, ["_id_", "x_1", "y_1"], [], {includeBuildUUIDs: true});
    return true;
});

validateCollectionsForTest(db);

// No more index build info, it gets finished this time.
validateIndexes(db, /*isIndexBuildDone=*/ true);

validateOplogEntries(db.getCollection("a"), /*numOps=*/ 15);

// Should see the newly committed prepared txn here.
assert.eq(1, db.getCollection("a").find({x: 500}).itcount());
assert.eq(1, db.getCollection("a").find({x: 501}).itcount());

rst.stopSet();
