/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {waitForCount} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

function smokeTestDLQ() {
    const uri = 'mongodb://' + db.getMongo().host;
    let connectionRegistry = [
        {name: "db1", type: 'atlas', options: {uri: uri}},
        {name: '__testMemory', type: 'in_memory', options: {}},
    ];
    const sp = new Streams(connectionRegistry);

    sp.createStreamProcessor("sp1", [
        {$source: {connectionName: '__testMemory'}},
        {$validate: {validator: {$expr: {$eq: ["$id", 0]}}, validationAction: "dlq"}},
        {$merge: {into: {connectionName: "db1", db: "test", coll: "validate1"}}}
    ]);

    // Start the streamProcessor.
    assert.commandWorked(
        sp.sp1.start({dlq: {connectionName: "db1", db: "test", coll: "validatedlq1"}}));

    let docs = [
        {id: 0, value: 1},
        {id: 1, value: 1},
        {id: 2, value: 1},
        {id: 3, value: 1},
        {id: 3, value: 1},
        {id: 3, value: 1},
        {id: 4, value: 1},
        {id: 0, value: 1},
        {id: 5, value: 1},
    ];
    assert.commandWorked(db.runCommand({streams_testOnlyInsert: '', name: "sp1", documents: docs}));

    // Validate the 2 docs with { id: 0 } are in the output collection.
    let coll = db.getSiblingDB("test").validate1;
    const expectedCount = 2;
    waitForCount(coll, expectedCount);
    let results = coll.find({});
    assert.eq(expectedCount, results.length());

    // Validate that 7 events are in the DLQ
    let dlqResults = db.getSiblingDB("test").validatedlq1.find({});
    assert.eq(7, dlqResults.length());

    // Stop the streamProcessor.
    assert.commandWorked(sp.sp1.stop());
}

smokeTestDLQ();
