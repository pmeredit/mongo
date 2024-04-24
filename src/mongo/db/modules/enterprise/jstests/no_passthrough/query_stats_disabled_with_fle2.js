/**
 * Test that query stats are not collected on encrypted collections.
 *  @tags: [requires_fcv_72]
 */

import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {getQueryStatsAggCmd, getQueryStatsFindCmd} from "jstests/libs/query_stats_utils.js";

function runTest(conn) {
    const docs = [
        {
            _id: 0,
            ssn: "123",
            uniqueFieldName: "A",
            manager: "B",
            age: NumberLong(25),
            location: [0, 0]
        },
        {
            _id: 1,
            ssn: "456",
            uniqueFieldName: "B",
            manager: "C",
            age: NumberLong(35),
            location: [0, 1]
        },
        {
            _id: 2,
            ssn: "789",
            uniqueFieldName: "C",
            manager: "D",
            age: NumberLong(45),
            location: [0, 2]
        },
        {
            _id: 3,
            ssn: "123",
            uniqueFieldName: "D",
            manager: "A",
            age: NumberLong(55),
            location: [0, 3]
        },
    ];

    const schema = {
        encryptedFields: {
            fields: [
                {path: "ssn", bsonType: "string", queries: {queryType: "equality"}},
                {path: "age", bsonType: "long", queries: {queryType: "equality"}}
            ]
        },
    };

    // Set up the non-encrypted collection.
    const dbName = "test";
    const testDB = conn.getDB(dbName);
    testDB.dropDatabase();
    var coll = testDB[jsTestName()];
    coll.drop();

    // Set up the encrypted collection.
    const encryptedCollName = jsTestName() + "_encrypted";
    let encryptedClient = new EncryptedClient(conn, testDB);
    let res = encryptedClient.createEncryptionCollection(encryptedCollName, schema);
    assert.commandWorked(res);
    let edb = encryptedClient.getDB();
    const encryptedColl = edb[encryptedCollName];

    for (const doc of docs) {
        assert.commandWorked(coll.insert(doc));
        assert.commandWorked(encryptedColl.einsert(doc));
    }

    // Test for aggregation requests that query stats are only collected when encryption is not
    // enabled.
    {
        const pipeline = [{$match: {_id: 0}}];
        const redactedPipeline = [{$match: {_id: {$eq: "?number"}}}];

        // Assert that telemetry is not collected on an aggregation on an encryption-enabled
        // collection.
        encryptedClient.runEncryptionOperation(
            () => { assert.eq(encryptedColl.aggregate(pipeline).itcount(), 1); });
        let queryStats = getQueryStatsAggCmd(testDB);
        assert.eq(0, queryStats.length, queryStats);

        // Assert that telemetry is collected on an aggregation on a regular collection.
        assert.eq(coll.aggregate(pipeline).itcount(), 1);
        queryStats = getQueryStatsAggCmd(testDB);
        assert.eq(1, queryStats.length);
        assert.eq({"db": `${dbName}`, "coll": `${jsTestName()}`},
                  queryStats[0].key.queryShape.cmdNs);
        assert.eq(redactedPipeline, queryStats[0].key.queryShape.pipeline);

        const encryptedPipeline = [{$match: {ssn: "456"}}];
        const redactedEncryptedPipeline = [{$match: {ssn: {$eq: "?string"}}}];

        // Assert that telemetry is not collected on an aggregation that queries an encrypted field.
        encryptedClient.runEncryptionOperation(
            () => { assert.eq(encryptedColl.aggregate(encryptedPipeline).itcount(), 1); });
        queryStats = getQueryStatsAggCmd(testDB);
        assert.eq(1,
                  queryStats.length);  // We still have the 1 previous query with stats collected.

        // Assert that telemetry is collected when querying the same field in a regular collection.
        encryptedClient.runEncryptionOperation(
            () => { assert.eq(coll.aggregate(encryptedPipeline).itcount(), 1); });
        queryStats = getQueryStatsAggCmd(testDB);
        assert.eq(2, queryStats.length);
        assert.eq({"db": `${dbName}`, "coll": `${jsTestName()}`},
                  queryStats[1].key.queryShape.cmdNs);
        assert.eq(redactedEncryptedPipeline, queryStats[1].key.queryShape.pipeline);

        // Assert that telemetry was collected on the previous calls to $queryStats, representative
        // of a collection-less aggregation. $queryStats is an aggregation without a targeted
        // collection, so the results of the $queryStats command should be visible in the telemetry
        // store.
        const results = testDB.adminCommand({
            aggregate: 1,
            pipeline:
                [{$queryStats: {}}, {$match: {"key.queryShape.cmdNs.coll": "$cmd.aggregate"}}],
            cursor: {}
        });
        assert.eq(1, results.cursor.firstBatch.length);
        queryStats = results.cursor.firstBatch;
        assert.eq({"db": "admin", "coll": "$cmd.aggregate"}, queryStats[0].key.queryShape.cmdNs);
        // This is the pipeline used by getQueryStatsAggCmd.
        assert.eq(
            [
                {$queryStats: {}},
                {
                    $match: {
                        $and: [
                            {"key.queryShape.command": {"$eq": "?string"}},
                            {"key.queryShape.pipeline.0.$queryStats": {$not: {$exists: "?bool"}}},
                            {"key.client.application.name": {"$eq": "?string"}}
                        ]
                    }
                },
                {$sort: {key: 1}}
            ],
            queryStats[0].key.queryShape.pipeline);
    }

    // Test for find requests that query stats are only collected when encryption is not
    // enabled.
    {
        const findCmd = {"manager": "D"};
        const redactedFindCmd = {"manager": {$eq: "?string"}};
        const findCmdOptions = {transformIdentifiers: false, collName: jsTestName()};

        // Assert that telemetry is not collected on a find command on an encryption-enabled
        // collection.
        encryptedClient.runEncryptionOperation(
            () => { assert.eq(encryptedColl.find(findCmd).itcount(), 1); });
        let queryStats = getQueryStatsFindCmd(testDB, findCmdOptions);
        assert.eq(0, queryStats.length, queryStats);
        // assert.eq({"db": "admin", "coll": "system.keys"}, queryStats[0].key.queryShape.cmdNs);
        // assert.eq({"db": "test", "coll": "keystore"}, queryStats[1].key.queryShape.cmdNs);

        // Assert that telemetry is collected on a find command on a regular collection.
        encryptedClient.runEncryptionOperation(
            () => { assert.eq(coll.find(findCmd).itcount(), 1); });
        queryStats = getQueryStatsFindCmd(testDB, findCmdOptions);
        assert.eq(1, queryStats.length, queryStats);
        assert.eq({"db": `${dbName}`, "coll": `${jsTestName()}`},
                  queryStats[0].key.queryShape.cmdNs);
        assert.eq(redactedFindCmd, queryStats[0].key.queryShape.filter);

        const encryptedFindCmd = {"ssn": "456"};
        const redactedEncryptedFindCmd = {"ssn": {$eq: "?string"}};

        // Assert that telemetry is not collected on a find command on an encryption-enabled
        // collection.
        encryptedClient.runEncryptionOperation(
            () => { assert.eq(encryptedColl.find(encryptedFindCmd).itcount(), 1); });
        queryStats = getQueryStatsFindCmd(testDB, findCmdOptions);
        // We still have the 1 previous query with stats collected.
        assert.eq(1, queryStats.length, queryStats);

        // Assert that telemetry is collected on a find command on a regular collection.
        encryptedClient.runEncryptionOperation(
            () => { assert.eq(coll.find(encryptedFindCmd).itcount(), 1); });
        queryStats = getQueryStatsFindCmd(testDB, findCmdOptions);
        assert.eq(2, queryStats.length, queryStats);
        assert.eq({"db": `${dbName}`, "coll": `${jsTestName()}`},
                  queryStats[1].key.queryShape.cmdNs);
        assert.eq(redactedEncryptedFindCmd, queryStats[1].key.queryShape.filter);
    }
}

const rst = new ReplSetTest({
    nodes: 1,
    nodeOptions: {
        setParameter: {internalQueryStatsRateLimit: -1},
    }
});
rst.startSet();
rst.initiate();
rst.awaitReplication();
const rstConn = rst.getPrimary();
runTest(rstConn);
rst.stopSet();

const st = new ShardingTest({
    mongos: 1,
    shards: 1,
    config: 1,
    rs: {nodes: 1},
    mongosOptions: {
        setParameter: {
            internalQueryStatsRateLimit: -1,
            'failpoint.skipClusterParameterRefresh': "{'mode':'alwaysOn'}"
        }
    }
});
runTest(st.s);
st.stop();