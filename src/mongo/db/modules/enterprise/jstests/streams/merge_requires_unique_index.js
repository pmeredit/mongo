// Tests that the $merge stage enforces that the "on" fields argument can be used to uniquely
// identify documents by checking that there is a supporting unique index.
import {ShardingTest} from "jstests/libs/shardingtest.js";
import {
    listStreamProcessors,
    sanitizeDoc,
    TEST_TENANT_ID,
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const st = new ShardingTest({shards: 2, rs: {nodes: 1}});

const mongosDB = st.s0.getDB("merge_requires_unique_index");
// Enable sharding on the DB and ensure that shard0 is the primary.
assert.commandWorked(
    mongosDB.adminCommand({enableSharding: mongosDB.getName(), primaryShard: st.rs0.getURL()}));

st.shard0.getDB("admin").setLogLevel(4, 'sharding');
st.shard1.getDB("admin").setLogLevel(4, 'sharding');

const outputColl = mongosDB.output_coll;
const dlqColl = mongosDB.dlq_coll;
const spName = "mergeTest";

function startStreamProcessor(pipeline, assertWorked = true) {
    const uri = 'mongodb://' + mongosDB.getMongo().host;
    let startCmd = {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: 'mergeTest1',
        pipeline: pipeline,
        connections: [
            {name: "db1", type: 'atlas', options: {uri: uri}},
            {name: '__testMemory', type: 'in_memory', options: {}},
        ],
        options: {
            dlq: {connectionName: "db1", db: mongosDB.getName(), coll: dlqColl.getName()},
            featureFlags: {}
        }
    };

    let result = db.runCommand(startCmd);
    jsTestLog(result);
    if (assertWorked) {
        assert.commandWorked(result);
    }
    return result;
}

function stopStreamProcessor() {
    let stopCmd = {
        streams_stopStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: spName,
    };
    let result = db.runCommand(stopCmd);
    assert.commandWorked(result);
}

function insertDocs(docs) {
    let insertCmd = {
        streams_testOnlyInsert: '',
        tenantId: TEST_TENANT_ID,
        name: spName,
        documents: docs,
    };

    let result = db.runCommand(insertCmd);
    jsTestLog(result);
    assert.commandWorked(result);
}

function resetOutputColl(shardKey, coll) {
    coll.drop();
    // Shard the target collection, and set the unique flag to ensure that there's a unique
    // index on the shard key.
    assert.commandWorked(
        mongosDB.adminCommand({shardCollection: coll.getFullName(), key: shardKey, unique: true}));
}

(function testWithoutOnFields() {
    jsTestLog("Running testWithoutOnFields");

    outputColl.drop();
    dlqColl.drop();

    let shardKey = {a: 1, b: 1};
    resetOutputColl(shardKey, outputColl);

    // Start a stream processor.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {connectionName: 'db1', db: mongosDB.getName(), coll: outputColl.getName()},
                whenMatched: 'replace',
                whenNotMatched: 'insert'
            }
        }
    ]);

    // Insert 2 documents (2 good, 1 bad) into the stream. The bad document contains an array in
    // shard key fields and should get added to the dlq.
    insertDocs([{a: 0, b: 0}, {x: 1, a: 0, b: [0]}, {a: 1, b: 1}]);

    assert.soon(() => { return outputColl.find().itcount() == 2; });
    assert.soon(() => {
        return tojson([{a: 0, b: 0}]) ==
            tojson(outputColl.find({a: 0}, {_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => {
        return tojson([{a: 1, b: 1}]) ==
            tojson(outputColl.find({a: 1}, {_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => { return dlqColl.find().itcount() == 1; });
    assert.eq(dlqColl.find({"operatorName": "MergeOperator"}).itcount(), 1);

    // Stop the streamProcessor.
    stopStreamProcessor();
})();

(function testOnFieldsMatchShardKey() {
    jsTestLog("Running testOnFieldsMatchShardKey");

    outputColl.drop();
    dlqColl.drop();

    let shardKey = {a: 1, b: 1};
    resetOutputColl(shardKey, outputColl);

    // Start a stream processor.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {connectionName: 'db1', db: mongosDB.getName(), coll: outputColl.getName()},
                whenMatched: 'replace',
                whenNotMatched: 'insert',
                on: ["a", "b"]
            }
        }
    ]);

    // Insert 2 documents (2 good, 1 bad) into the stream. The bad document contains an array in
    // shard key fields and should get added to the dlq.
    insertDocs([{a: 0, b: 0, c: 0}, {x: 1, a: 0, b: [0]}, {a: 1, b: 1, c: 1}]);

    assert.soon(() => { return outputColl.find().itcount() == 2; });
    assert.soon(() => {
        return tojson([{a: 0, b: 0, c: 0}]) ==
            tojson(outputColl.find({a: 0}, {_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => {
        return tojson([{a: 1, b: 1, c: 1}]) ==
            tojson(outputColl.find({a: 1}, {_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => { return dlqColl.find().itcount() == 1; });
    assert.eq(dlqColl.find({"operatorName": "MergeOperator"}).itcount(), 1);

    // Insert one more doc to actually replace an existing doc.
    insertDocs([{a: 0, b: 0, c: 1}]);

    assert.soon(() => {
        return tojson([{a: 0, b: 0, c: 1}]) ==
            tojson(outputColl.find({a: 0}, {_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => { return outputColl.find().itcount() == 2; });

    // Stop the streamProcessor.
    stopStreamProcessor();
})();

(function testOnFieldsMatchShardKeyDynamicColls() {
    jsTestLog("Running testOnFieldsMatchShardKeyDynamicColls");

    let shardKey = {a: 1, b: 1};

    let colls = [];
    for (let i = 0; i < 2; i++) {
        const coll = mongosDB.getCollection("coll" + i);
        coll.drop();
        resetOutputColl(shardKey, coll);
        colls.push(coll);
    }
    dlqColl.drop();

    // Start a stream processor.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {
                    connectionName: 'db1',
                    db: mongosDB.getName(),
                    coll: {$concat: ["coll", {$toString: "$a"}]},
                },
                whenMatched: 'replace',
                whenNotMatched: 'insert',
                on: ["a", "b"]
            }
        }
    ]);

    // Insert 2 documents (2 good, 1 bad) into the stream. The bad document contains an array in
    // shard key fields and should get added to the dlq.
    insertDocs([{a: 0, b: 0, c: 0}, {x: 1, a: 0, b: [0]}, {a: 1, b: 1, c: 1}]);

    assert.soon(() => { return colls[0].find().itcount() == 1; });
    assert.soon(() => { return colls[1].find().itcount() == 1; });
    assert.soon(() => {
        return tojson([{a: 0, b: 0, c: 0}]) ==
            tojson(colls[0].find({a: 0}, {_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => {
        return tojson([{a: 1, b: 1, c: 1}]) ==
            tojson(colls[1].find({a: 1}, {_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => { return dlqColl.find().itcount() == 1; });
    assert.eq(dlqColl.find({"operatorName": "MergeOperator"}).itcount(), 1);

    // Insert one more doc to actually replace an existing doc.
    insertDocs([{a: 0, b: 0, c: 1}]);

    assert.soon(() => {
        return tojson([{a: 0, b: 0, c: 1}]) ==
            tojson(colls[0].find({a: 0}, {_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => { return colls[0].find().itcount() == 1; });

    // Stop the streamProcessor.
    stopStreamProcessor();
})();

(function testOnFieldsContainShardKeyAndIndexDoesNotExist() {
    jsTestLog("Running testOnFieldsContainShardKeyAndIndexDoesNotExist");

    outputColl.drop();
    dlqColl.drop();

    let shardKey = {a: 1, b: 1};
    resetOutputColl(shardKey, outputColl);

    // Start a stream processor.
    let result = startStreamProcessor(
        [
            {$source: {'connectionName': '__testMemory'}},
            {
                $merge: {
                    into:
                        {connectionName: 'db1', db: mongosDB.getName(), coll: outputColl.getName()},
                    whenMatched: 'replace',
                    whenNotMatched: 'insert',
                    on: ["a", "b", "c"]
                }
            }
        ],
        false /*assertWorked*/);
    assert.commandFailedWithCode(result, ErrorCodes.StreamProcessorInvalidOptions);
    jsTestLog(result);
    assert.eq(result.errorLabels[0], "StreamProcessorUserError");
})();

(function testOnFieldsContainShardKeyAndIndexIsNotUnique() {
    jsTestLog("Running testOnFieldsContainShardKeyAndIndexIsNotUnique");

    outputColl.drop();
    dlqColl.drop();

    let shardKey = {a: 1, b: 1};
    resetOutputColl(shardKey, outputColl);
    assert.commandWorked(outputColl.createIndex({a: 1, b: 1, c: 1}));

    // Start a stream processor.
    let result = startStreamProcessor(
        [
            {$source: {'connectionName': '__testMemory'}},
            {
                $merge: {
                    into:
                        {connectionName: 'db1', db: mongosDB.getName(), coll: outputColl.getName()},
                    whenMatched: 'replace',
                    whenNotMatched: 'insert',
                    on: ["a", "b", "c"]
                }
            }
        ],
        false /*assertWorked*/);
    assert.commandFailedWithCode(result, ErrorCodes.StreamProcessorInvalidOptions);
    jsTestLog(result);
    assert.eq(result.errorLabels[0], "StreamProcessorUserError");
})();

(function testOnFieldsContainShardKeyAndIndexExists() {
    jsTestLog("Running testOnFieldsContainShardKeyAndIndexExists");

    outputColl.drop();
    dlqColl.drop();

    let shardKey = {a: 1, b: 1};
    resetOutputColl(shardKey, outputColl);
    assert.commandWorked(outputColl.createIndex({a: 1, b: 1, c: 1}, {unique: true}));

    // Start a stream processor.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {connectionName: 'db1', db: mongosDB.getName(), coll: outputColl.getName()},
                whenMatched: 'replace',
                whenNotMatched: 'insert',
                on: ["a", "b", "c"]
            }
        }
    ]);

    // Insert 2 documents (2 good, 1 bad) into the stream. The bad document contains an array in
    // shard key fields and should get added to the dlq.
    insertDocs([{a: 0, b: 0, c: 0}, {x: 1, a: 0, b: [0]}, {a: 1, b: 1, c: 1}]);

    assert.soon(() => { return outputColl.find().itcount() == 2; });
    assert.soon(() => {
        return tojson([{a: 0, b: 0, c: 0}]) ==
            tojson(outputColl.find({a: 0}, {_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => {
        return tojson([{a: 1, b: 1, c: 1}]) ==
            tojson(outputColl.find({a: 1}, {_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => { return dlqColl.find().itcount() == 1; });
    assert.eq(dlqColl.find({"operatorName": "MergeOperator"}).itcount(), 1);

    // Stop the streamProcessor.
    stopStreamProcessor();
})();

(function testShardKeyIsIdField() {
    jsTestLog("Running testShardKeyIsIdField");

    outputColl.drop();
    dlqColl.drop();

    let shardKey = {_id: 1};
    resetOutputColl(shardKey, outputColl);

    // Start a stream processor.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {connectionName: 'db1', db: mongosDB.getName(), coll: outputColl.getName()},
                whenMatched: 'replace',
                whenNotMatched: 'insert',
                on: ["_id"]
            }
        }
    ]);

    // Insert 3 documents into the stream. The document without explicit _id will have an
    // automatically generated one.
    insertDocs([{_id: 0, a: 0, b: 0}, {x: 1}, {_id: 1, a: 1, b: 1}]);

    assert.soon(() => { return outputColl.find().itcount() == 3; });
    assert.soon(() => {
        return tojson([{_id: 0, a: 0, b: 0}]) ==
            tojson(outputColl.find({a: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => {
        return tojson([{_id: 1, a: 1, b: 1}]) ==
            tojson(outputColl.find({a: 1}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => {
        return tojson([{x: 1}]) ==
            tojson(outputColl.find({x: 1}, {_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => { return dlqColl.find().itcount() == 0; });

    // Stop the streamProcessor.
    stopStreamProcessor();
})();

(function testShardKeyIsIdFieldWithoutOnFields() {
    jsTestLog("Running testShardKeyIsIdFieldWithoutOnFields");

    outputColl.drop();
    dlqColl.drop();

    let shardKey = {_id: 1};
    resetOutputColl(shardKey, outputColl);

    // Start a stream processor.
    startStreamProcessor([
        {$source: {'connectionName': '__testMemory'}},
        {
            $merge: {
                into: {connectionName: 'db1', db: mongosDB.getName(), coll: outputColl.getName()},
                whenMatched: 'replace',
                whenNotMatched: 'insert'
            }
        }
    ]);

    // Insert 3 documents into the stream. The document without explicit _id will have an
    // automatically generated one.
    insertDocs([{_id: 0, a: 0, b: 0}, {x: 1}, {_id: 1, a: 1, b: 1}]);

    assert.soon(() => { return outputColl.find().itcount() == 3; });
    assert.soon(() => {
        return tojson([{_id: 0, a: 0, b: 0}]) ==
            tojson(outputColl.find({a: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => {
        return tojson([{_id: 1, a: 1, b: 1}]) ==
            tojson(outputColl.find({a: 1}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => {
        return tojson([{x: 1}]) ==
            tojson(outputColl.find({x: 1}, {_id: 0}).toArray().map((doc) => sanitizeDoc(doc)));
    });
    assert.soon(() => { return dlqColl.find().itcount() == 0; });

    // Stop the streamProcessor.
    stopStreamProcessor();
})();

st.stop();
assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
