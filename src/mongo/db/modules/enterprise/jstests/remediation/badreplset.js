/**
 * badreplset.js
 * This script creates a replica set with documents that are different on the various nodes.
 * It then pauses and allows repairs to be made, while the set is still being written to.
 * When the user hits a key it continues, removes documents that were intentionally unresolvable,
 * and checks the replica set for consistency.
 *
 * The intended use is testing the repair_checked_replset.js script. It is currently not intended to
 * be run from evergreen.
 */
import {Thread} from "jstests/libs/parallelTester.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

function writeLoadFunc(host, dbName, collName, shutdownLatch) {
    const nodeDB = new Mongo(host).getDB(dbName);
    const coll = nodeDB[collName];
    while (shutdownLatch.getCount() > 0) {
        assert.commandWorked(coll.insert({desc: "Document is OK", status: "good"}));
        sleep(100);
    }
}

function dbCheckCompleted(db) {
    return db.currentOp().inprog.filter(x => x["desc"] == "dbCheck")[0] === undefined;
}

// Wait for dbCheck to complete (on both primaries and secondaries).  Fails an assertion if
// dbCheck takes longer than maxMs.
function awaitDbCheckCompletion(rst, db) {
    let start = Date.now();

    assert.soon(() => dbCheckCompleted(db), "dbCheck timed out");
    rst.awaitSecondaryNodes();
    rst.awaitReplication();

    // Give the health log buffers some time to flush.
    sleep(100);
}

const nodeCount = 3;
// TestData.enableTestCommands = false
let rst = new ReplSetTest({
    name: TestData.testName,
    nodes: [{}, {}, {}, {rsConfig: {arbiterOnly: true}}],
    nodeOptions: {setParameter: {oplogApplicationEnforcesSteadyStateConstraints: false}}
});
rst.startSet();
rst.initiateWithHighElectionTimeout();
let dbName = TestData.testName;
let collName = "test";
const primary = rst.getPrimary();
const primaryDb = primary.getDB(dbName);
const primaryColl = primaryDb[collName];
let secondary = rst.getSecondaries()[0];
let tiebreaker = rst.getSecondaries()[1];

const docOk = 0;
const docMissingPrimaryShouldExist = 1;
const docMissingPrimaryShouldNotExist = 2;
const docMissingPrimaryDifferentOnTiebreaker = 3;
const docMissingSecondaryShouldExist = 4;
const docMissingSecondaryShouldNotExist = 5;
const docMissingSecondaryDifferentOnTiebreaker = 6;
const docDifferentCorrectOnPrimary = 7;
const docDifferentCorrectOnSecondary = 8;
const docDifferentAllNodes = 9;

if (TestData.auth) {
    // ClusterMonitor for replSetGetStatus, applyOps for _internalReadAtClusterTime,
    // readWriteAnyDatabase for repairs.
    primaryDb.getSiblingDB("admin").createRole({
        role: "corruptAdmin",
        roles: ["clusterMonitor", "readWriteAnyDatabase"],
        privileges: [
            {resource: {cluster: true}, actions: ["applyOps"]},
            {resource: {db: "local", collection: "system.healthlog"}, actions: ["find"]},
            {
                resource: {db: "config", collection: "unhealthyRanges"},
                actions: [
                    "find",
                    "insert",
                    "update",
                    "remove",
                    "createCollection",
                    "dropCollection",
                    "createIndex",
                    "dropIndex"
                ]
            }
        ]
    });
    primaryDb.getSiblingDB("admin").createUser(
        {user: "admin", pwd: "password", roles: ["corruptAdmin"]});
}

assert.commandWorked(primaryColl.insert({_id: docOk, desc: "Document is OK", status: "good"}));
assert.commandWorked(primaryColl.insert({
    _id: docMissingSecondaryShouldExist,
    desc: "Document is missing on secondary, should exist",
    status: "good"
}));
assert.commandWorked(primaryColl.insert({
    _id: docMissingSecondaryShouldNotExist,
    desc: "Document is missing on secondary, should not exist",
    status: "bad"
}));
assert.commandWorked(primaryColl.insert({
    _id: docMissingSecondaryDifferentOnTiebreaker,
    desc: "Document is missing on secondary, different on tiebreaker",
    status: "indeterminate",
    node: "primary"
}));
assert.commandWorked(primaryColl.insert({
    _id: docDifferentCorrectOnPrimary,
    desc: "Document is different and correct on primary",
    status: "good"
}));
assert.commandWorked(primaryColl.insert({
    _id: docDifferentCorrectOnSecondary,
    desc: "Document is different and correct on secondary",
    status: "bad"
}));
assert.commandWorked(primaryColl.insert({
    _id: docDifferentAllNodes,
    desc: "Document is different on all nodes",
    status: "indeterminate",
    node: "primary"
}));
rst.awaitReplication();

{
    jsTestLog("Stopping secondary to corrupt the set");
    rst.stop(secondary, {forRestart: true});
    const standaloneSecondary =
        MongoRunner.runMongod({dbpath: secondary.dbpath, noCleanData: true});
    const standaloneDb = standaloneSecondary.getDB(dbName);
    const standaloneColl = standaloneDb[collName];
    assert.commandWorked(standaloneColl.insert({
        _id: docMissingPrimaryShouldExist,
        desc: "Document is missing on primary, should exist",
        status: "good"
    }));
    assert.commandWorked(standaloneColl.insert({
        _id: docMissingPrimaryShouldNotExist,
        desc: "Document is missing on primary, should not exist",
        status: "bad"
    }));
    assert.commandWorked(standaloneColl.insert({
        _id: docMissingPrimaryDifferentOnTiebreaker,
        desc: "Document is missing on primary, different on secondary and tiebreaker",
        status: "indeterminate",
        node: "secondary"
    }));
    assert.commandWorked(standaloneColl.remove({_id: docMissingSecondaryShouldExist}));
    assert.commandWorked(standaloneColl.remove({_id: docMissingSecondaryShouldNotExist}));
    assert.commandWorked(standaloneColl.remove({_id: docMissingSecondaryDifferentOnTiebreaker}));
    assert.commandWorked(standaloneColl.replaceOne(
        {_id: docDifferentCorrectOnPrimary},
        {desc: "Document is different and correct on primary", status: "bad"}));
    assert.commandWorked(standaloneColl.replaceOne(
        {_id: docDifferentCorrectOnSecondary},
        {desc: "Document is different and correct on secondary", status: "good"}));
    assert.commandWorked(standaloneColl.replaceOne(
        {_id: docDifferentAllNodes},
        {desc: "Document is different on all nodes", status: "indeterminate", node: "secondary"}));
    MongoRunner.stopMongod(standaloneSecondary);
}
jsTestLog("Restarting secondary");
secondary = rst.start(secondary, {}, true /* restart */);
rst.awaitSecondaryNodes();

jsTestLog("Stopping tiebreaker to corrupt the set");
{
    rst.stop(tiebreaker, {forRestart: true});
    const standaloneTiebreaker =
        MongoRunner.runMongod({dbpath: tiebreaker.dbpath, noCleanData: true});
    const standaloneDb = standaloneTiebreaker.getDB(dbName);
    const standaloneColl = standaloneDb[collName];
    assert.commandWorked(standaloneColl.insert({
        _id: docMissingPrimaryShouldExist,
        desc: "Document is missing on primary, should exist",
        status: "good"
    }));
    assert.commandWorked(standaloneColl.insert({
        _id: docMissingPrimaryDifferentOnTiebreaker,
        desc: "Document is missing on primary, different on secondary and tiebreaker",
        status: "indeterminate",
        node: "tiebreaker"
    }));
    assert.commandWorked(standaloneColl.remove({_id: docMissingSecondaryShouldNotExist}));
    assert.commandWorked(
        standaloneColl.replaceOne({_id: docMissingSecondaryDifferentOnTiebreaker}, {
            _id: docMissingSecondaryDifferentOnTiebreaker,
            desc: "Document is missing on secondary, different on tiebreaker",
            status: "indeterminate",
            node: "tiebreaker"
        }));
    assert.commandWorked(standaloneColl.replaceOne(
        {_id: docDifferentCorrectOnSecondary},
        {desc: "Document is different and correct on secondary", status: "good"}));
    assert.commandWorked(standaloneColl.replaceOne(
        {_id: docDifferentAllNodes},
        {desc: "Document is different on all nodes", status: "indeterminate", node: "tiebreaker"}));
    MongoRunner.stopMongod(standaloneTiebreaker);
}

jsTestLog("Restarting tiebreaker");
tiebreaker = rst.start(tiebreaker, {}, true /* restart */);
rst.awaitSecondaryNodes();

jsTestLog("Secondary restarted, verifying corruption");
const secondaryDb = secondary.getDB(dbName);
const secondaryColl = secondaryDb[collName];

const statusProj = {
    _id: 0,
    status: 1
};
assert.eq(primaryColl.find({_id: docMissingPrimaryShouldExist}).toArray(), []);
assert.eq(primaryColl.find({_id: docMissingPrimaryShouldNotExist}).toArray(), []);
assert.eq(primaryColl.find({_id: docMissingSecondaryShouldExist}, statusProj).toArray(),
          [{status: "good"}]);
assert.eq(primaryColl.find({_id: docMissingSecondaryShouldNotExist}, statusProj).toArray(),
          [{status: "bad"}]);
assert.eq(primaryColl.find({_id: docDifferentCorrectOnPrimary}, statusProj).toArray(),
          [{status: "good"}]);
assert.eq(primaryColl.find({_id: docDifferentCorrectOnSecondary}, statusProj).toArray(),
          [{status: "bad"}]);

assert.eq(secondaryColl.find({_id: docMissingPrimaryShouldExist}, statusProj).toArray(),
          [{status: "good"}]);
assert.eq(secondaryColl.find({_id: docMissingPrimaryShouldNotExist}, statusProj).toArray(),
          [{status: "bad"}]);
assert.eq(secondaryColl.find({_id: docMissingSecondaryShouldExist}).toArray(), []);
assert.eq(secondaryColl.find({_id: docMissingSecondaryShouldNotExist}).toArray(), []);
assert.eq(secondaryColl.find({_id: docDifferentCorrectOnPrimary}, statusProj).toArray(),
          [{status: "bad"}]);
assert.eq(secondaryColl.find({_id: docDifferentCorrectOnSecondary}, statusProj).toArray(),
          [{status: "good"}]);

assert.commandWorked(primaryDb.runCommand({dbCheck: collName, minKey: 0, maxKey: 5}));
awaitDbCheckCompletion(rst, primaryDb);
assert.commandWorked(primaryDb.runCommand({dbCheck: collName, minKey: 5, maxCount: 10}));
awaitDbCheckCompletion(rst, primaryDb);

jsTestLog("Corruption is verified, attempting to repair");

function repairDoc(coll, docOrId, deleteDoc) {
    if (deleteDoc) {
        assert.commandWorked(coll.replaceOne(docOrId, {special: "deleted"}, {upsert: true}));
        assert.commandWorked(coll.remove(docOrId));
    } else {
        assert.commandWorked(coll.remove(docOrId));
        assert.commandWorked(coll.replaceOne({_id: docOrId._id}, docOrId, {upsert: true}));
    }
}

let shutdownLatch = new CountDownLatch(1);
let writeLoadThread = new Thread(writeLoadFunc, primary.host, dbName, collName, shutdownLatch);
writeLoadThread.start();
jsTestLog("Time to repair the replica set");
print("Start your repair on " + primary.host);
print("Then hit enter to continue");
passwordPrompt();
shutdownLatch.countDown();
writeLoadThread.join();

jsTestLog("Verifing repairs: secondary");
rst.awaitReplication();

assert.eq(secondaryColl.find({_id: docMissingPrimaryShouldExist}, statusProj).toArray(),
          [{status: "good"}]);
assert.eq(secondaryColl.find({_id: docMissingPrimaryShouldNotExist}).toArray(), []);
assert.eq(secondaryColl.find({_id: docMissingSecondaryShouldExist}, statusProj).toArray(),
          [{status: "good"}]);
assert.eq(secondaryColl.find({_id: docMissingSecondaryShouldNotExist}).toArray(), []);
assert.eq(secondaryColl.find({_id: docDifferentCorrectOnPrimary}, statusProj).toArray(),
          [{status: "good"}]);
assert.eq(secondaryColl.find({_id: docDifferentCorrectOnSecondary}, statusProj).toArray(),
          [{status: "good"}]);
assert.eq(secondaryColl.find({status: "bad"}).itcount(), 0);

jsTestLog("Removing indeterminate documents so dbhash passes");
repairDoc(primaryColl, {_id: docMissingPrimaryDifferentOnTiebreaker}, true);
repairDoc(primaryColl, {_id: docMissingSecondaryDifferentOnTiebreaker}, true);
repairDoc(primaryColl, {_id: docDifferentAllNodes}, true);

rst.stopSet();
