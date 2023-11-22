/**
 * Test that bulkWrite emit the same metrics that corresponding calls to update, delete and insert.
 *
 * @tags: [featureFlagBulkWriteCommand] // TODO SERVER-52419: Remove this tag.
 */

import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {BulkWriteMetricChecker} from "jstests/libs/bulk_write_utils.js";

function runTest(replTest, bulkWrite) {
    const dbName = "testDB";
    const collName = "testColl";
    const namespace = `${dbName}.${collName}`;
    const primary = replTest.getPrimary();
    const session = primary.startSession();
    let testDB = session.getDatabase(dbName);
    testDB.dropDatabase();

    let client = new EncryptedClient(testDB.getMongo(), dbName);

    assert.commandWorked(client.createEncryptionCollection(collName, {
        validator: {$jsonSchema: {required: ["x"]}},
        encryptedFields: {
            "fields": [
                {"path": "x", "bsonType": "string", "queries": {"queryType": "equality"}},
                {"path": "y", "bsonType": "string"},
            ]
        }
    }));

    testDB = client.getDB();
    const coll = testDB[collName];

    // Simplifies implementation of checkBulkWriteMetrics:
    // totals["testDB.testColl"] will not be undefined on first top call below.
    coll.insert({_id: 99, x: "0"});

    const metricChecker = new BulkWriteMetricChecker(testDB, namespace, bulkWrite, true /*fle*/);

    metricChecker.checkMetrics("Simple insert.",
                               [{insert: 0, document: {_id: 0, x: "0"}}],
                               [{insert: collName, documents: [{_id: 0, x: "0"}]}],
                               {inserted: 3, keysExamined: 2});

    metricChecker.checkMetrics(
        "Simple Update.",
        [{update: 0, filter: {_id: 0, x: "0"}, updateMods: {$set: {x: "0", y: "1"}}}],
        [{update: collName, updates: [{q: {_id: 0, x: "0"}, u: {$set: {x: "0", y: "1"}}}]}],
        {updated: 2, inserted: 2, keysExamined: 9});

    assert.commandWorked(coll.insert({_id: 1, x: "0", a: [{b: 5}, {b: 1}, {b: 2}]},
                                     {writeConcern: {w: "majority"}}));

    metricChecker.checkMetrics(
        "Update with arrayFilters.",
        [{
            update: 0,
            filter: {_id: 1},
            updateMods: {$set: {"a.$[i].b": 6, x: "1"}},
            arrayFilters: [{"i.b": 5}]
        }],
        [{
            update: collName,
            updates: [{q: {_id: 1}, u: {$set: {"a.$[i].b": 6, x: "1"}}, arrayFilters: [{"i.b": 5}]}]
        }],
        {updated: 2, inserted: 2, updateArrayFilters: 0, keysExamined: 5});
    metricChecker.checkMetrics("Simple delete.",
                               [{delete: 0, filter: {_id: 0}}],
                               [{delete: collName, deletes: [{q: {_id: 0}, limit: 1}]}],
                               {deleted: 1, keysExamined: 1});

    metricChecker.checkMetricsWithRetries("Simple insert with retry.",
                                          [{insert: 0, document: {_id: 3, x: "0"}}],
                                          {
                                              insert: collName,
                                              documents: [{_id: 3, x: "0"}],
                                          },
                                          {
                                              inserted: 3,
                                              retriedInsert: 1,
                                              retriedCommandsCount: 3 + (bulkWrite ? 1 : 0),
                                              retriedStatementsCount: 3,
                                              keysExamined: 4
                                          },
                                          session.getSessionId(),
                                          NumberLong(10));

    metricChecker.checkMetricsWithRetries(
        "Simple update with retry.",
        [{
            update: 0,
            filter: {_id: 1},
            updateMods: {$set: {"a.$[i].b": 7, x: "2"}},
            arrayFilters: [{"i.b": 6}]
        }],
        {
            update: collName,
            updates: [{q: {_id: 1}, u: {$set: {"a.$[i].b": 7, x: "2"}}, arrayFilters: [{"i.b": 6}]}]
        },
        {
            updated: 2,
            inserted: 2,
            retriedCommandsCount: 3 + (bulkWrite ? 1 : 0),
            retriedStatementsCount: 3,
            updateArrayFilters: 0,
            keysExamined: 8
        },
        session.getSessionId(),
        NumberLong(11));

    metricChecker.checkMetrics("Multiple inserts.",
                               [
                                   {insert: 0, document: {_id: 4, x: "0"}},
                                   {insert: 0, document: {_id: 5, x: "0"}},
                                   {insert: 0, document: {_id: 6, x: "0"}},
                               ],
                               [
                                   {insert: collName, documents: [{_id: 4, x: "0"}]},
                                   {insert: collName, documents: [{_id: 5, x: "0"}]},
                                   {insert: collName, documents: [{_id: 6, x: "0"}]},
                               ],
                               {inserted: 9, keysExamined: 6});

    coll.drop();
}

const testName = jsTestName();
const replTest = new ReplSetTest({
    name: testName,
    nodes: [{}, {rsConfig: {priority: 0}}],
    nodeOptions: {
        setParameter: {
            // Required for serverStatus() to have opWriteConcernCounters.
            reportOpWriteConcernCountersInServerStatus: true
        }
    }
});

replTest.startSet();
replTest.initiateWithHighElectionTimeout();

for (const bulkWrite of [true, false]) {
    runTest(replTest, bulkWrite);
}

replTest.stopSet();
