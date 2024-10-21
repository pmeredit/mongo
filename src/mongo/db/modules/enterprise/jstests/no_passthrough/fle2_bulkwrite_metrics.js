/**
 * Test that bulkWrite emit the same metrics that corresponding calls to update, delete and insert.
 *
 * @tags: [requires_fcv_80]
 */

import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {BulkWriteMetricChecker} from "jstests/libs/bulk_write_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

function runTest(isMongos, cluster, bulkWrite, retryCount) {
    // We are ok with the randomness here since we clearly log the state.
    const errorsOnly = Math.random() < 0.5;
    print(`Running on a ${isMongos ? "ShardingTest" : "ReplSetTest"} with bulkWrite = ${
        bulkWrite} and errorsOnly = ${errorsOnly}.`);

    const dbName = "testDB";
    const collName = "testColl";
    const namespace = `${dbName}.${collName}`;
    const session = isMongos ? cluster.s.startSession() : cluster.getPrimary().startSession();
    let testDB = session.getDatabase(dbName);
    testDB.dropDatabase();

    let client = new EncryptedClient(testDB.getMongo(), dbName);

    // ui: un-indexed encrypted fields
    // i: indexed encrypted fields
    assert.commandWorked(client.createEncryptionCollection(collName, {
        validator: {$jsonSchema: {required: ["i2"]}},
        encryptedFields: {
            "fields": [
                {"path": "ui0", "bsonType": "string"},
                {"path": "ui1", "bsonType": "string"},
                {"path": "ui2", "bsonType": "string"},
                {"path": "i0", "bsonType": "string", "queries": {"queryType": "equality"}},
                {"path": "i1", "bsonType": "string", "queries": {"queryType": "equality"}},
                {"path": "i2", "bsonType": "string", "queries": {"queryType": "equality"}},
            ]
        }
    }));

    if (isMongos) {
        assert.commandWorked(testDB.adminCommand({'enableSharding': dbName}));
        assert.commandWorked(testDB.adminCommand({shardCollection: namespace, key: {z: 1}}));
        assert.commandWorked(testDB.adminCommand({split: namespace, middle: {z: 5}}));
        assert.commandWorked(testDB.adminCommand({split: namespace, middle: {z: 200}}));

        // Move chunks so each shard has one chunk.
        assert.commandWorked(testDB.adminCommand({
            moveChunk: namespace,
            find: {z: 2},
            to: cluster.shard0.shardName,
            _waitForDelete: true
        }));
        assert.commandWorked(testDB.adminCommand({
            moveChunk: namespace,
            find: {z: 200},
            to: cluster.shard2.shardName,
            _waitForDelete: true
        }));
    }

    testDB = client.getDB();
    const coll = testDB[collName];

    // Simplifies implementation of checkBulkWriteMetrics:
    // totals["testDB.testColl"] will not be undefined on first top call below.
    coll.einsert({_id: 99, i2: "0"});

    const metricChecker = new BulkWriteMetricChecker(testDB,
                                                     [namespace],
                                                     bulkWrite,
                                                     isMongos,
                                                     true /*fle*/,
                                                     errorsOnly,
                                                     retryCount,
                                                     false /*timeseries*/);

    client.runEncryptionOperation(() => {
        metricChecker.checkMetrics("Simple insert",
                                   [{insert: 0, document: {_id: 0, i1: "0", i2: "0"}}],
                                   [{insert: collName, documents: [{_id: 0, i1: "0", i2: "0"}]}],
                                   {inserted: 1, eqIndexedEncryptedFields: 2});

        metricChecker.checkMetrics(
            "Simple Update",
            [{
                update: 0,
                filter: {_id: 0},
                updateMods: {$set: {i0: "0", i1: "0", ui1: "0", i2: "0", ui2: "1"}}
            }],
            [{
                update: collName,
                updates: [{q: {_id: 0}, u: {$set: {i0: "0", i1: "0", ui1: "0", i2: "0", ui2: "1"}}}]
            }],
            {updated: 1, eqIndexedEncryptedFields: 3});

        metricChecker.checkMetrics(
            "Simple Update of Unindexed Encrypted Field",
            [{update: 0, filter: {_id: 0}, updateMods: {$set: {ui2: "2"}}}],
            [{update: collName, updates: [{q: {_id: 0}, u: {$set: {ui2: "2"}}}]}],
            {updated: 1, eqIndexedEncryptedFields: 0});

        assert.commandWorked(coll.insert({_id: 1, i2: "0", a: [{b: 5}, {b: 1}, {b: 2}]},
                                         {writeConcern: {w: "majority"}}));

        metricChecker.checkMetrics(
            "Update with arrayFilters",
            [{
                update: 0,
                filter: {_id: 1},
                updateMods: {$set: {"a.$[i].b": 6, i2: "1"}},
                arrayFilters: [{"i.b": 5}]
            }],
            [{
                update: collName,
                updates:
                    [{q: {_id: 1}, u: {$set: {"a.$[i].b": 6, i2: "1"}}, arrayFilters: [{"i.b": 5}]}]
            }],
            {updated: 1, eqIndexedEncryptedFields: 1, updateArrayFilters: isMongos ? 1 : 0});

        metricChecker.checkMetrics("Simple delete",
                                   [{delete: 0, filter: {_id: 0}}],
                                   [{delete: collName, deletes: [{q: {_id: 0}, limit: 1}]}],
                                   {deleted: 1});

        metricChecker.checkMetricsWithRetries(
            "Simple insert with retry",
            [{insert: 0, document: {_id: 3, i1: "0", i2: "0"}}],
            {
                insert: collName,
                documents: [{_id: 3, i1: "0", i2: "0"}],
            },
            {inserted: 1, eqIndexedEncryptedFields: 2, retriedInsert: 1},
            session.getSessionId(),
            NumberLong(10));

        metricChecker.checkMetricsWithRetries(
            "Simple update with retry",
            [{
                update: 0,
                filter: {_id: 1},
                updateMods: {$set: {"a.$[i].b": 7, i0: "2", i1: "2", i2: "2", ui2: "3"}},
                arrayFilters: [{"i.b": 6}]
            }],
            {
                update: collName,
                updates: [{
                    q: {_id: 1},
                    u: {$set: {"a.$[i].b": 7, i0: "2", i1: "2", i2: "2", ui2: "3"}},
                    arrayFilters: [{"i.b": 6}]
                }]
            },
            {
                updated: 1,
                eqIndexedEncryptedFields: 3,
                updateArrayFilters: isMongos ? retryCount
                                             : 0  // This is incremented even on a retry.
            },
            session.getSessionId(),
            NumberLong(11));

        metricChecker.checkMetricsWithRetries(
            "Simple Update of Unindexed Encrypted Field with retry",
            [{update: 0, filter: {_id: 1}, updateMods: {$set: {ui0: "3", ui1: "3", ui2: "3"}}}],
            {update: collName, updates: [{q: {_id: 1}, u: {$set: {ui0: "3", ui1: "3", ui2: "3"}}}]},
            {
                updated: 1,
                eqIndexedEncryptedFields: 0,
                updateArrayFilters: 0,
            },
            session.getSessionId(),
            NumberLong(12));

        metricChecker.checkMetricsWithRetries(
            "Simple Update without encrypted field with retry",
            [{update: 0, filter: {_id: 1}, updateMods: {$set: {a: "0"}}}],
            {update: collName, updates: [{q: {_id: 1}, u: {$set: {a: "0"}}}]},
            {
                updated: 1,
                eqIndexedEncryptedFields: 0,
                updateArrayFilters: 0,
            },
            session.getSessionId(),
            NumberLong(13));

        metricChecker.checkMetrics(
            "Multiple inserts",
            [
                {insert: 0, document: {_id: 4, i0: "2", i1: "2", i2: "0", ui2: "3"}},
                {insert: 0, document: {_id: 5, i0: "2", i1: "2", i2: "0", ui2: "3"}},
                {insert: 0, document: {_id: 6, i0: "2", i1: "2", i2: "0", ui2: "3"}},
                {insert: 0, document: {_id: 7, i0: "2", i1: "2", i2: "0", ui2: "3"}},
            ],
            [
                {insert: collName, documents: [{_id: 4, i0: "2", i1: "2", i2: "0", ui2: "3"}]},
                {insert: collName, documents: [{_id: 5, i0: "2", i1: "2", i2: "0", ui2: "3"}]},
                {insert: collName, documents: [{_id: 6, i0: "2", i1: "2", i2: "0", ui2: "3"}]},
                {insert: collName, documents: [{_id: 7, i0: "2", i1: "2", i2: "0", ui2: "3"}]},
            ],
            {inserted: 4, eqIndexedEncryptedFields: 3});
    });

    coll.drop();
}

{
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
    replTest.initiate();

    const retryCount = 3;
    for (const bulkWrite of [false, true]) {
        runTest(false /* isMongos */, replTest, bulkWrite, retryCount);
    }

    replTest.stopSet();
}

{
    const st = new ShardingTest({mongos: 1, shards: 3, rs: {nodes: 1}, config: 1});

    const retryCount = 3;
    for (const bulkWrite of [false, true]) {
        runTest(true /* isMongos */, st, bulkWrite, retryCount);
    }

    st.stop();
}
