import {
    connectionName,
    dbName,
    dlqCollName,
    getOperatorStats,
    listStreamProcessors,
    runStreamProcessorWindowTest
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const outColl1 = db.getSiblingDB(dbName).outColl1;
const outColl2 = db.getSiblingDB(dbName).outColl2;
const dlqColl = db.getSiblingDB(dbName)[dlqCollName];
const spName = "largeGroupAccumulatorTest";
const buildInfo = getBuildInfo();

outColl1.drop();
outColl2.drop();

function getGroupPipeline(pipeline) {
    return {
        $tumblingWindow: {
            interval: {size: NumberInt(1), unit: "second"},
            allowedLateness: NumberInt(0),
            pipeline: pipeline
        }
    };
}

function testLargeAccumulator(pipeline, source, sink, verifyAction) {
    runStreamProcessorWindowTest({
        spName: spName,
        pipeline: [source, getGroupPipeline(pipeline), sink],
        verifyAction: verifyAction
    });
}

// Test 1
// Insert 1 document to the change stream
// The pipeline generates 100 documents of size 48KB that accumulates state >4MB for a single group
// key Merge operator receives a single Document size ~ 4.5MB
testLargeAccumulator(
    [
        {$project: {docSize: 1, value: {$range: [0, "$docCount"]}}},
        {$unwind: "$value"},
        {$project: {bigValue: {$range: [0, "$docSize"]}}},
        {$unwind: "$bigValue"},
        {$group: {_id: "$_id", bigArr: {$push: "$bigValue"}}},
    ],
    {
        $source: {
            connectionName: connectionName,
            db: dbName,
            coll: db.getSiblingDB(dbName).inputColl1.getName(),
            timeField: {$toDate: {$multiply: ['$ts', 1000]}},
            config: {'fullDocument': 'required', 'fullDocumentOnly': true}
        }
    },
    {$merge: {into: {connectionName: connectionName, db: dbName, coll: outColl1.getName()}}},
    () => {
        let inputColl = db.getSiblingDB(dbName).inputColl1;
        inputColl.insert(
            {_id: 1, ts: 1, docCount: 100, docSize: 3000});  // will generate 100 - 48KB documents
        inputColl.insert({_id: 3, ts: 3, docCount: 1, docSize: 1});

        assert.soon(() => { return outColl1.count() == 1; });
        assert.eq(dlqColl.count(), 0);

        let changeStreamOpStats = getOperatorStats(spName, "ChangeStreamConsumerOperator");
        assert.eq(changeStreamOpStats["inputMessageCount"], 2);
        assert.eq(changeStreamOpStats["outputMessageCount"], 2);

        let GroupOpStats = getOperatorStats(spName, "GroupOperator");
        assert.eq(GroupOpStats["inputMessageCount"], 300001);
        assert.eq(GroupOpStats["outputMessageCount"], 1);

        let mergeOpStats = getOperatorStats(spName, "MergeOperator");
        assert.eq(mergeOpStats["inputMessageCount"], 1);
        assert.eq(mergeOpStats["dlqMessageCount"], 0);
        assert.eq(mergeOpStats["dlqMessageSize"], 0);
        assert.gt(mergeOpStats["inputMessageSize"], 4 * 1024 * 1024);
        inputColl.drop();
    });

// Test 2 : testing the negative case
// Insert 1 document to the change stream
// Internally the pipeline generates 200 documents of size 32KB that accumulates
// 18MB state for a single group key
// Merge operator receives a Document of size >16MB (limit) will dlq it
testLargeAccumulator(
    [
        {$project: {docSize: 1, value: {$range: [0, "$docCount"]}}},
        {$unwind: "$value"},
        {$project: {bigValue: {$range: [0, "$docSize"]}}},
        {$unwind: "$bigValue"},
        {$group: {_id: "$_id", bigArr: {$push: "$bigValue"}}},
    ],
    {
        $source: {
            connectionName: connectionName,
            db: dbName,
            coll: db.getSiblingDB(dbName).inputColl2.getName(),
            timeField: {$toDate: {$multiply: ['$ts', 1000]}},
            config: {'fullDocument': 'required', 'fullDocumentOnly': true}
        }
    },
    {$merge: {into: {connectionName: connectionName, db: dbName, coll: outColl2.getName()}}},
    () => {
        let inputColl = db.getSiblingDB(dbName).inputColl2;

        inputColl.insert({_id: 1, ts: 1, docCount: 240, docSize: 6000});
        inputColl.insert({_id: 3, ts: 3, docCount: 1, docSize: 1});

        assert.soon(() => { return dlqColl.count() == 1; });
        var dlqRes = dlqColl.find();
        assert.eq(dlqRes[0].operatorName, "MergeOperator");
        assert.eq(outColl2.count(), 0);

        let mergeOpStats = getOperatorStats(spName, "MergeOperator");
        assert.soon(() => {
            mergeOpStats = getOperatorStats(spName, "MergeOperator");
            return mergeOpStats["dlqMessageCount"] == 1;
        });
        assert.eq(mergeOpStats["inputMessageCount"], 1);
        assert.gt(mergeOpStats["dlqMessageSize"], 0);
        assert.gt(mergeOpStats["inputMessageSize"], 18 * 1024 * 1024);  // > 16MB

        let GroupOpStats = getOperatorStats(spName, "GroupOperator");
        assert.eq(GroupOpStats["inputMessageCount"], 1440001);
        assert.eq(GroupOpStats["outputMessageCount"], 1);

        let changeStreamOpStats = getOperatorStats(spName, "ChangeStreamConsumerOperator");
        assert.eq(changeStreamOpStats["inputMessageCount"], 2);
        assert.eq(changeStreamOpStats["outputMessageCount"], 2);
        inputColl.drop();
    });

// Test 3
// Insert 1 document that the pipeline uses to generate 20 documents of 1MB size
// to accumulates 20MB+ state for a single group key
// emit to in-memory sink
testLargeAccumulator(
    [
        { $project: { docSize: 1, seed: 1, value: { $range: [ 0, "$docCount" ] } } },
        { $unwind: "$value" },
        { $project: { seed: 1, bigValue: { $range: [0, "$docSize"] }}},
        { 
            $project: 
            { 
                bigStr: 
                { 
                    $reduce: { 
                        input: "$bigValue",
                        initialValue: "",
                        in: {
                            "$concat": [ "$$value", "$seed" ]
                        }
                    }
                }
            }
        },
        {
            $group: {
                _id: "$_id",
                bigArr: { $push: "$bigStr" }
            }
        },
    ],
    {
        $source: {
            connectionName: connectionName,
            db: dbName,
            coll: db.getSiblingDB(dbName).inputColl4.getName(),
            timeField: {$toDate: {$multiply: ['$ts', 1000]}},
            config: {
                'fullDocument': 'required',
                'fullDocumentOnly': true
            }
        }
    },
    { 
        $emit: {connectionName: '__testMemory'} 
    },
    () => {
        let inputColl = db.getSiblingDB(dbName).inputColl4;
        
        const seed = Array(1024 + 1).toString(); // 1KB 
        inputColl.insert({_id: 1, ts: 1, docCount: 20, docSize: 1000, seed: seed}); // will generate 20 - 1MB documents
        inputColl.insert({_id: 3, ts: 3, docCount: 1, docSize: 1000, seed: seed});
        
        let inMemoryOpStats = getOperatorStats(spName, "InMemorySinkOperator");
        assert.soon(() => { 
            inMemoryOpStats = getOperatorStats(spName, "InMemorySinkOperator");
            return inMemoryOpStats["inputMessageCount"] == 1;
        });
        // No dlq's
        assert.eq(inMemoryOpStats["dlqMessageCount"], 0);
        assert.eq(inMemoryOpStats["dlqMessageSize"], 0);
        assert.gt(inMemoryOpStats["stateSize"], inMemoryOpStats["inputMessageSize"]);
        assert.eq(dlqColl.count(), 0);
        assert.eq(inMemoryOpStats["inputMessageSize"], inMemoryOpStats["outputMessageSize"]);

        
        let GroupOpStats = getOperatorStats(spName, "GroupOperator");
        assert.eq(GroupOpStats["inputMessageCount"], 21);
        assert.eq(GroupOpStats["outputMessageCount"], 1);

        let changeStreamOpStats = getOperatorStats(spName, "ChangeStreamConsumerOperator");
        assert.eq(changeStreamOpStats["inputMessageCount"], 2);
        assert.eq(changeStreamOpStats["outputMessageCount"], 2);
        inputColl.drop();
    }
);

// Skip running the following tests for a debug build.
// The following tests generates large amount of data > 200MB.
if (!buildInfo.debug) {
    // Test 4
    // Insert 1 document that the pipeline uses to generate 20 documents of 10MB size
    // to accumulates 200MB+ state for a single group key
    // emit it to the in-memory sink
    // dlq because of the size limit on the push accumulator (100MB)
    testLargeAccumulator(
        [
            { $project: { docSize: 1, seed: 1, value: { $range: [ 0, "$docCount" ] } } },
            { $unwind: "$value" },
            { $project: { seed: 1, bigValue: { $range: [0, "$docSize"] }}},
            { 
                $project: 
                { 
                    bigStr: 
                    { 
                        $reduce: { 
                            input: "$bigValue",
                            initialValue: "",
                            in: {
                                "$concat": [ "$$value", "$seed" ]
                            }
                        }
                    }
                }
            },
            {
                $group: {
                    _id: "$_id",
                    bigArr: { $push: "$bigStr" }
                }
            },
        ],
        {
            $source: {
                connectionName: connectionName,
                db: dbName,
                coll: db.getSiblingDB(dbName).inputColl5.getName(),
                timeField: {$toDate: {$multiply: ['$ts', 1000]}},
                config: {
                    'fullDocument': 'required',
                    'fullDocumentOnly': true
                }
            }
        },
        { 
            $emit: {connectionName: '__testMemory'} 
        },
        () => {
            let inputColl = db.getSiblingDB(dbName).inputColl5;

            const seed = Array(1024 + 1).toString(); // 10KB 
            inputColl.insert({_id: 1, ts: 1, docCount: 200, docSize: 1000, seed: seed}); // will generate 20 - 10MB documents
            inputColl.insert({_id: 3, ts: 3, docCount: 1, docSize: 1, seed: seed});
                    
            assert.soon(() => { return dlqColl.count() == 1; });
            var dlqRes = dlqColl.find();
            assert.eq(dlqRes[0].operatorName, "GroupOperator");

            let inMemoryOpStats = getOperatorStats(spName, "InMemorySinkOperator");
            assert.eq(inMemoryOpStats["inputMessageCount"], 0);        
            assert.eq(inMemoryOpStats["inputMessageSize"], inMemoryOpStats["stateSize"]);
            assert.eq(inMemoryOpStats["maxMemoryUsage"], inMemoryOpStats["stateSize"]);
            
            let GroupOpStats = getOperatorStats(spName, "GroupOperator");
            assert.eq(GroupOpStats["inputMessageCount"], 201);
            assert.eq(GroupOpStats["outputMessageCount"], 0);
            assert.eq(GroupOpStats["dlqMessageCount"], 1);
            assert.gt(GroupOpStats["dlqMessageSize"], 0);


            let changeStreamOpStats = getOperatorStats(spName, "ChangeStreamConsumerOperator");
            assert.eq(changeStreamOpStats["inputMessageCount"], 2);
            assert.eq(changeStreamOpStats["outputMessageCount"], 2);
            inputColl.drop();
        }
    );

    // Test 5
    // Insert 1 document that the pipeline uses to generate 20 documents of 10MB size
    // to accumulates 200MB+ state for a single group key
    // emit it to the in-memory sink with an updated push accumulator size limit to 250MB
    testLargeAccumulator(
        [
            { $project: { docSize: 1, seed: 1, value: { $range: [ 0, "$docCount" ] } } },
            { $unwind: "$value" },
            { $project: { seed: 1, bigValue: { $range: [0, "$docSize"] }}},
            { 
                $project: 
                { 
                    bigStr: 
                    { 
                        $reduce: { 
                            input: "$bigValue",
                            initialValue: "",
                            in: {
                                "$concat": [ "$$value", "$seed" ]
                            }
                        }
                    }
                }
            },
            {
                $group: {
                    _id: "$_id",
                    bigArr: { $push: "$bigStr" }
                }
            },
        ],
        {
            $source: {
                connectionName: connectionName,
                db: dbName,
                coll: db.getSiblingDB(dbName).inputColl6.getName(),
                timeField: {$toDate: {$multiply: ['$ts', 1000]}},
                config: {
                    'fullDocument': 'required',
                    'fullDocumentOnly': true
                }
            }
        },
        { 
            $emit: {connectionName: '__testMemory'} 
        },
        () => {
            let inputColl = db.getSiblingDB(dbName).inputColl6;
            // update the memory limit for the accumulator
            let oldLimit = db.adminCommand({getParameter: 1, internalQueryMaxPushBytes: 1})["internalQueryMaxPushBytes"];
            let newLimit = 250 * 1024 * 1024;
            assert.commandWorked(db.adminCommand({setParameter: 1, internalQueryMaxPushBytes: newLimit}));

            const seed = Array(1024 + 1).toString(); // 1KB 
            inputColl.insert({_id: 1, ts: 1, docCount: 200, docSize: 1024, seed: seed}); // will generate 20 - 10MB documents
            inputColl.insert({_id: 3, ts: 3, docCount: 1, docSize: 1000, seed: seed});
            
            let inMemoryOpStats = getOperatorStats(spName, "InMemorySinkOperator");
            assert.soon(() => { 
                inMemoryOpStats = getOperatorStats(spName, "InMemorySinkOperator");
                return inMemoryOpStats["inputMessageCount"] == 1;
            });
            // No dlq's
            assert.eq(inMemoryOpStats["dlqMessageCount"], 0);
            assert.eq(inMemoryOpStats["dlqMessageSize"], 0);
            assert.gt(inMemoryOpStats["stateSize"], inMemoryOpStats["inputMessageSize"]);
            assert.eq(dlqColl.count(), 0);
            assert.eq(inMemoryOpStats["inputMessageSize"], inMemoryOpStats["outputMessageSize"]);
            
            let totalSinkBytes = 200 * 1024 * 1024; // ~200MB
            assert.gt(inMemoryOpStats["inputMessageSize"], totalSinkBytes);

            let GroupOpStats = getOperatorStats(spName, "GroupOperator");
            assert.eq(GroupOpStats["inputMessageCount"], 201);
            assert.eq(GroupOpStats["outputMessageCount"], 1);
            
            let changeStreamOpStats = getOperatorStats(spName, "ChangeStreamConsumerOperator");
            assert.eq(changeStreamOpStats["inputMessageCount"], 2);
            assert.eq(changeStreamOpStats["outputMessageCount"], 2);
            inputColl.drop();
            assert.commandWorked(db.adminCommand({setParameter: 1, internalQueryMaxPushBytes: oldLimit}));
        }
    );

    // Test 6
    // Insert 1 document that the pipeline uses to generate 200 documents of 9MB size
    // to accumulates 1.8GB+ state for a single group key
    // emit it to the in-memory sink with an updated push accumulator size limit to 2GB
    testLargeAccumulator(
        [
            { $project: { docSize: 1, seed: 1, value: { $range: [ 0, "$docCount" ] } } },
            { $unwind: "$value" },
            { $project: { seed: 1, bigValue: { $range: [0, "$docSize"] }}},
            { 
                $project: 
                { 
                    bigStr: 
                    { 
                        $reduce: { 
                            input: "$bigValue",
                            initialValue: "",
                            in: {
                                "$concat": [ "$$value", "$seed" ]
                            }
                        }
                    }
                }
            },
            {
                $group: {
                    _id: "$_id",
                    bigArr: { $push: "$bigStr" }
                }
            },
        ],
        {
            $source: {
                connectionName: connectionName,
                db: dbName, 
                coll: db.getSiblingDB(dbName).inputColl7.getName(),
                timeField: {$toDate: {$multiply: ['$ts', 1000]}},
                config: {
                    'fullDocument': 'required',
                    'fullDocumentOnly': true
                }
            }
        },
        { 
            $emit: {connectionName: '__testMemory'} 
        },
        () => {
            let inputColl = db.getSiblingDB(dbName).inputColl7;
            // update the memory limit for the accumulator
            let oldLimit = db.adminCommand({getParameter: 1, internalQueryMaxPushBytes: 1})["internalQueryMaxPushBytes"];
            let newLimit = 2 * 1024 * 1024 * 1024 - 1;
            assert.commandWorked(db.adminCommand({setParameter: 1, internalQueryMaxPushBytes: newLimit}));

            const seed = Array(96 * 1024 + 1).toString();
            inputColl.insert({_id: 1, ts: 1, docCount: 2000, docSize: 10, seed: seed}); // will generate 200 - 9MB documents
            inputColl.insert({_id: 3, ts: 3, docCount: 1, docSize: 1, seed: "."});
            
            sleep(10000);
            let inMemoryOpStats = getOperatorStats(spName, "InMemorySinkOperator");
            assert.soon(() => { 
                inMemoryOpStats = getOperatorStats(spName, "InMemorySinkOperator");
                return inMemoryOpStats["inputMessageCount"] == 1;
            });
            // No dlq's
            assert.eq(inMemoryOpStats["dlqMessageCount"], 0);
            assert.eq(inMemoryOpStats["dlqMessageSize"], 0);
            assert.gt(inMemoryOpStats["stateSize"], inMemoryOpStats["inputMessageSize"]);
            assert.eq(dlqColl.count(), 0);
            assert.eq(inMemoryOpStats["inputMessageSize"], inMemoryOpStats["outputMessageSize"]);
    
            let totalSinkBytes = 1.8 * 1024 * 1024 * 1024; // ~1.8G
            assert.gt(inMemoryOpStats["inputMessageSize"], totalSinkBytes);

            let GroupOpStats = getOperatorStats(spName, "GroupOperator");
            assert.eq(GroupOpStats["inputMessageCount"], 2001);
            assert.eq(GroupOpStats["outputMessageCount"], 1);

            let changeStreamOpStats = getOperatorStats(spName, "ChangeStreamConsumerOperator");
            assert.eq(changeStreamOpStats["inputMessageCount"], 2);
            assert.eq(changeStreamOpStats["outputMessageCount"], 2);
            inputColl.drop();
            assert.commandWorked(db.adminCommand({setParameter: 1, internalQueryMaxPushBytes: oldLimit}));
        }
    );
}

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);