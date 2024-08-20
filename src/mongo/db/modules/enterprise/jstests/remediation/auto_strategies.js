/**
 * auto_strategies.js
 *
 * This script creates a replica set with the collections expected to be found after runnning
 * scan_checked_replset.js over a corrupted 5-node replica set.  It then runs
 * repair_checked_documents.py with each of the remediation options, and checks that remediation
 * occurred as expected.
 *
 * Note this test does not actually create a corrupt replset, nor does it check actual remediation.
 *
 */
import {getPython3Binary} from "jstests/libs/python.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

const strategyNames = [
    "majority",
    "majorityDeletePluralityKeep",
    "majorityKeepPluralityDelete",
    "plurality",
    "majorityKeepNeverDelete",
    "pluralityKeepNeverDelete",
    "majorityDeleteNeverKeep",
    "pluralityDeleteNeverKeep",
];

const missingDocMarker = "dbcheck_docWasMissing";
const NC = "no change";
const rangeCollName = "unhealthyRanges";
const metadataDbName = "config";
const saveCollPrefix = "dbcheck.";
const testCollName = "test";
const scriptUnderTest = "src/mongo/db/modules/enterprise/src/scripts/repair_checked_documents.py";

let python_binary = getPython3Binary();

const testDocs = [
    // Consistent documents shouldn't appear in the scan, but if they do, they should behave
    // properly.
    {
        versions: [1, 1, 1, 1, 1],
        correctVersion: [1, 1, 1, 1, 1, 1, NC, NC],
        msg: "Consistent document"
    },

    // Majority documents
    {
        versions: [1, 1, 1, 1, 2],
        correctVersion: [1, 1, 1, 1, 1, 1, NC, NC],
        msg: "Two versions 4:1"
    },
    {
        versions: [1, 1, 1, 1, null],
        correctVersion: [1, 1, 1, 1, 1, 1, NC, NC],
        msg: "Four node same, one missing"
    },
    {
        versions: [1, 1, 1, 2, 2],
        correctVersion: [1, 1, 1, 1, 1, 1, NC, NC],
        msg: "Two versions 3:2"
    },
    {
        versions: [1, 1, 1, 2, 3],
        correctVersion: [1, 1, 1, 1, 1, 1, NC, NC],
        msg: "Three versions 3:1:1"
    },
    {
        versions: [1, 1, 1, 2, null],
        correctVersion: [1, 1, 1, 1, 1, 1, NC, NC],
        msg: "Two versions 3:1:1 missing"
    },
    {
        versions: [1, 1, 1, null, null],
        correctVersion: [1, 1, 1, 1, 1, 1, NC, NC],
        msg: "Three same, two missing"
    },

    // Consistent missing
    {
        versions: [null, null, null, null, null],
        correctVersion: [null, null, null, null, NC, NC, null, null],
        msg: "Consistent missing document"
    },

    // Majority missing
    {
        versions: [null, null, null, null, 1],
        correctVersion: [null, null, null, null, NC, NC, null, null],
        msg: "4 missing:1"
    },
    {
        versions: [null, null, null, 1, 1],
        correctVersion: [null, null, null, null, NC, NC, null, null],
        msg: "3 missing:2"
    },
    {
        versions: [null, null, null, 1, 2],
        correctVersion: [null, null, null, null, NC, NC, null, null],
        msg: "3 missing:1:1"
    },

    // Two present, no majority.
    {versions: [1, 1, 2, 2, 3], correctVersion: [NC, NC, NC, NC, NC, NC, NC, NC], msg: "2:2:1"},
    {
        versions: [1, 1, 2, 2, null],
        correctVersion: [NC, NC, NC, NC, NC, NC, NC, NC],
        msg: "2:2:1 missing"
    },
    {
        versions: [1, 1, null, null, 2],
        correctVersion: [NC, 1, null, NC, NC, 1, NC, null],
        msg: "2:2 missing:1"
    },
    {versions: [1, 1, 2, 3, 4], correctVersion: [NC, 1, NC, 1, NC, 1, NC, NC], msg: "2:1:1:1"},

    // Two missing, no majority (other cases are covered above)
    {
        versions: [1, 2, 3, null, null],
        correctVersion: [NC, NC, null, null, NC, NC, NC, null],
        msg: "1:1:1:2 missing"
    },

    // All different
    {
        versions: [1, 2, 3, 4, 5],
        correctVersion: [NC, NC, NC, NC, NC, NC, NC, NC],
        msg: "All different"
    },
    {
        versions: [1, 2, 3, 4, null],
        correctVersion: [NC, NC, null, NC, NC, NC, NC, null],
        msg: "All different one missing"
    },
];

function createTestData(db) {
    let startId = 1;
    let endId = startId;
    let saveCollNames = [];
    db[testCollName].drop();
    for (let i = 1; i <= 5; i++) {
        let name = saveCollPrefix + testCollName + "." + i;
        db[name].drop();
        saveCollNames.push(name);
    }

    for (let testDoc of testDocs) {
        let unchangedDoc = {_id: endId, msg: testDoc.msg, version: NC};
        assert.commandWorked(db[testCollName].insert(unchangedDoc));
        for (let i = 0; i < 5; i++) {
            let saveCollName = saveCollNames[i];
            let doc;
            if (testDoc.versions[i] === null) {
                doc = {_id: endId};
                doc[missingDocMarker] = 1;
            } else
                doc = {_id: endId, msg: testDoc.msg, version: testDoc.versions[i]};
            assert.commandWorked(db[saveCollName].insert(doc));
        }
        endId++;
    }
    let metadataDb = db.getSiblingDB(metadataDbName);
    metadataDb[rangeCollName].remove({"_id.db": db.getName(), "_id.collection": testCollName});
    assert.commandWorked(metadataDb[rangeCollName].insert({
        _id: {db: db.getName(), collection: testCollName, minKey: startId, maxKey: endId},
        scanned: true,
        scanCollections: saveCollNames
    }));
}

function checkTestDataResults(db, strategy, dryrun = false) {
    let currentId = 1;
    let wrongDocs = 0;
    const strategyName = strategyNames[strategy];
    for (let testDoc of testDocs) {
        let correctDoc;
        let resultDoc = db[testCollName].findOne({_id: currentId});
        let correctVersion = NC;
        if (!dryrun)
            correctVersion = testDoc.correctVersion[strategy];
        if (correctVersion !== null) {
            correctDoc = {_id: currentId, msg: testDoc.msg, version: correctVersion};
        }
        if (resultDoc === null) {
            if (correctVersion !== null) {
                wrongDocs++;
                printjson({
                    error: "Document is missing and should be present",
                    strategy: strategyName,
                    correctDoc: correctDoc
                });
            }
        } else if (correctVersion === null) {
            wrongDocs++;
            printjson({
                error: "Document is present and should be deleted",
                strategy: strategyName,
                resultDoc: resultDoc
            });
        } else if (bsonWoCompare(resultDoc, correctDoc) != 0) {
            wrongDocs++;
            printjson({
                error: "Document is present but has incorrect contents",
                strategy: strategyName,
                resultDoc: resultDoc,
                correctDoc: correctDoc
            });
        }
        currentId++;
    }
    assert.eq(0, wrongDocs, "There were " + wrongDocs + " wrong docs for strategy " + strategyName);
}

let rst = new ReplSetTest({name: TestData.testName, nodes: 1});
rst.startSet();
rst.initiateWithHighElectionTimeout();
let dbName = TestData.testName;
const primary = rst.getPrimary();
const primaryDb = primary.getDB(dbName);
let mongoUri = "mongodb://" + primary.host;
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
                resource: {db: metadataDbName, collection: rangeCollName},
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
    mongoUri = "mongodb://admin:password@" + primary.host;
}

for (let strategy = 0; strategy < strategyNames.length; strategy++) {
    createTestData(primaryDb);
    assert.eq(0,
              runProgram(python_binary,
                         scriptUnderTest,
                         "--no-dryrun",
                         "--strategy",
                         strategyNames[strategy],
                         mongoUri));
    checkTestDataResults(primaryDb, strategy);
}

// The verbose output is not tested; this merely tests that the repair still works with the
// verbose option enabled.
for (let strategy = 0; strategy < strategyNames.length; strategy++) {
    createTestData(primaryDb);
    assert.eq(0,
              runProgram(python_binary,
                         scriptUnderTest,
                         "--no-dryrun",
                         "--verbose",
                         "--strategy",
                         strategyNames[strategy],
                         mongoUri));
    checkTestDataResults(primaryDb, strategy);
}

// The dryrun output is not tested; this merely tests that the repair is not done with dry run
// enabled.
for (let strategy = 0; strategy < strategyNames.length; strategy++) {
    createTestData(primaryDb);
    assert.eq(0,
              runProgram(
                  python_binary, scriptUnderTest, "--strategy", strategyNames[strategy], mongoUri));
    checkTestDataResults(primaryDb, strategy, true /* dryrun*/);
}

rst.stopSet();
