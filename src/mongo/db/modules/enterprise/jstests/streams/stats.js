/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    listStreamProcessors,
    sink,
    TEST_TENANT_ID
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

(function() {

(function testGetStats_Basic() {
    const sp = new Streams(TEST_TENANT_ID, [
        {name: 'db', type: 'atlas', options: {uri: `mongodb://${db.getMongo().host}`}},
        {
            name: 'kafka',
            type: 'kafka',
            options: {bootstrapServers: 'localhost:9092', isTestKafka: true}
        },
    ]);
    const source = {
        $source: {
            connectionName: 'kafka',
            topic: 'topic',
            timeField: {$dateFromString: {'dateString': '$timestamp'}},
            testOnlyPartitionCount: NumberInt(1),
        },
    };
    const aggregation = {
        $tumblingWindow: {
            interval: {size: NumberInt(1), unit: 'second'},
            pipeline: [{
                $group: {
                    _id: '$id',
                    sum: {$sum: '$value'},
                },
            }],
        },
    };
    const stream = sp.createStreamProcessor('sp0', [source, aggregation, sink.memory]);

    stream.start({featureFlags: {}});

    const documents = [
        // Two documents with same grouping key (`id`) and `timestamp`, so
        // they should fall into the same window and the same group within that
        // window, so only one document should be produced for the following
        // two input documents from the window operator.
        {timestamp: "2023-03-03T20:42:29.000Z", id: 1, value: 1},
        {timestamp: "2023-03-03T20:42:29.000Z", id: 1, value: 1},

        {timestamp: "2023-03-03T20:42:30.000Z", id: 2, value: 1},
    ];
    stream.testInsert(...documents);

    // This will close all the windows created from the first document set that
    // was sent above.
    const commitDocument = {timestamp: "2023-03-03T20:43:00.000Z", id: 3, value: 1};
    stream.testInsert(commitDocument);

    // Wait for the two documents to be emitted.
    assert.soon(() => {
        jsTestLog(stream.stats());
        return stream.stats()['outputMessageCount'] == 2;
    });

    const metrics = sp.metrics();
    const stats = stream.stats(false /* verbose */);
    jsTestLog(stats);
    assert.eq(4, stats['inputMessageCount']);
    assert.eq(stats['inputMessageCount'],
              metrics['counters'].find((c) => c.name === 'num_input_documents').value);
    assert.gt(stats['inputMessageSize'], 0);
    assert.eq(stats['inputMessageSize'],
              metrics['counters'].find((c) => c.name === 'num_input_bytes').value);
    assert.eq(2, stats['outputMessageCount']);
    assert.eq(stats['outputMessageCount'],
              metrics['counters'].find((c) => c.name === 'num_output_documents').value);
    assert.gt(stats['outputMessageSize'], 0);
    assert.eq(stats['outputMessageSize'],
              metrics['counters'].find((c) => c.name === 'num_output_bytes').value);
    assert.gt(stats['stateSize'], 0);
    assert.eq(stats['watermark'], ISODate('2023-03-03T20:42:59.999Z'));
    assert.eq(stats['dlqMessageCount'],
              metrics['counters'].find((c) => c.name === 'num_dlq_documents').value);
    assert.eq(stats['dlqMessageSize'],
              metrics['counters'].find((c) => c.name === 'num_dlq_bytes').value);
    const verboseStats = stream.stats(true /* verbose */);
    jsTestLog(verboseStats);

    // Make sure that the summary stats emitted in `verbose` mode match
    // the summary stats emitted in the non-verbose mode.
    assert.eq(stats['inputMessageCount'], verboseStats['inputMessageCount']);
    assert.eq(stats['inputMessageSize'], verboseStats['inputMessageSize']);
    assert.eq(stats['outputMessageCount'], verboseStats['outputMessageCount']);
    assert.eq(stats['outputMessageSize'], verboseStats['outputMessageSize']);
    assert.eq(stats['stateSize'], verboseStats['stateSize']);

    // Make sure that watermarks are written at a partition level
    assert.eq(1, verboseStats['kafkaPartitions'].length);
    assert.eq(verboseStats['kafkaPartitions'][0]['watermark'], ISODate('2023-03-03T20:42:59.999Z'));

    assert.eq(3, verboseStats['operatorStats'].length);

    // The source operator specific stats should match with the summary input stats.
    const sourceStats = verboseStats['operatorStats'][0];
    assert.eq('KafkaConsumerOperator', sourceStats['name']);
    assert.eq(verboseStats['inputMessageCount'], sourceStats['inputMessageCount']);
    assert.eq(verboseStats['inputMessageSize'], sourceStats['inputMessageSize']);
    assert.eq(0, sourceStats['dlqMessageCount']);
    assert.eq(0, sourceStats['dlqMessageSize']);
    assert.eq(0, sourceStats['stateSize']);

    // Input and output for the source operator should be the same since its
    // just funneling whatever it receives out to the next operator.
    assert.eq(sourceStats['inputMessageCount'], sourceStats['outputMessageCount']);

    const groupStats = verboseStats['operatorStats'][1];
    assert.eq('GroupOperator', groupStats['name']);
    assert.eq(4, groupStats['inputMessageCount']);
    assert.eq(2, groupStats['outputMessageCount']);

    // Group operator memory usage should align with the memory usage in
    // the stream summary stats, since the Group operator is the only stateful
    // operator.
    assert.eq(144, groupStats['stateSize']);

    // The sink operator specific stats should match with the summary output stats.
    const sinkStats = verboseStats['operatorStats'][2];
    assert.eq('InMemorySinkOperator', sinkStats['name']);
    assert.eq(verboseStats['outputMessageCount'], sinkStats['inputMessageCount']);
    assert.eq(verboseStats['outputMessageSize'], sinkStats['inputMessageSize']);
    assert.eq(2216, sinkStats['stateSize']);

    const totalStateSize =
        verboseStats["operatorStats"].reduce((sum, stats) => sum + stats["stateSize"], 0);
    assert.eq(totalStateSize, verboseStats['stateSize']);

    // Verify scaled stats match expected results.
    const scaleFactor = 10;
    const verboseScaledStats = stream.stats(true /* verbose */, scaleFactor);
    jsTestLog(verboseScaledStats);
    assert.eq(verboseScaledStats['inputMessageSize'],
              verboseStats['inputMessageSize'] / scaleFactor);
    assert.eq(verboseScaledStats['outputMessageSize'],
              verboseStats['outputMessageSize'] / scaleFactor);
    assert.eq(verboseScaledStats['stateSize'], verboseStats['stateSize'] / scaleFactor);
    assert.eq(verboseScaledStats['memoryTrackerBytes'],
              verboseStats['memoryTrackerBytes'] / scaleFactor);
    const sourceScaledStats = verboseScaledStats['operatorStats'][0];
    assert.eq(sourceScaledStats['inputMessageSize'], sourceStats['inputMessageSize'] / scaleFactor);
    const groupScaledStats = verboseScaledStats['operatorStats'][1];
    assert.eq(groupScaledStats['stateSize'], groupStats['stateSize'] / scaleFactor);
    const sinkScaledStats = verboseScaledStats['operatorStats'][2];
    assert.eq(sinkScaledStats['stateSize'], sinkStats['stateSize'] / scaleFactor);

    stream.stop();
})();

(function testGetStats_BasicMultiplePartitions() {
    const sp = new Streams(TEST_TENANT_ID, [
        {name: 'db', type: 'atlas', options: {uri: `mongodb://${db.getMongo().host}`}},
        {
            name: 'kafka',
            type: 'kafka',
            options: {bootstrapServers: 'localhost:9092', isTestKafka: true}
        },
    ]);
    const source = {
        $source: {
            connectionName: 'kafka',
            topic: 'topic',
            timeField: {$dateFromString: {'dateString': '$timestamp'}},
            testOnlyPartitionCount: NumberInt(3),
        },
    };
    const aggregation = {
        $tumblingWindow: {
            interval: {size: NumberInt(1), unit: 'second'},
            pipeline: [{
                $group: {
                    _id: '$id',
                    sum: {$sum: '$value'},
                },
            }],
        },
    };
    const stream = sp.createStreamProcessor('sp0', [source, aggregation, sink.memory]);

    stream.start({featureFlags: {}});

    const documents = [
        // Two documents with same grouping key (`id`) and `timestamp`, so
        // they should fall into the same window and the same group within that
        // window, so only one document should be produced for the following
        // two input documents from the window operator.
        {timestamp: "2023-03-03T20:42:29.000Z", id: 1, value: 1},
        {timestamp: "2023-03-03T20:42:29.000Z", id: 1, value: 1},

        {timestamp: "2023-03-03T20:42:30.000Z", id: 2, value: 1},
    ];
    stream.testInsert(...documents);

    // This will close all the windows created from the first document set that
    // was sent above.
    const commitDocuments = [
        {timestamp: "2023-03-03T20:43:00.000Z", id: 1, value: 2},
        {timestamp: "2023-03-03T20:43:00.000Z", id: 2, value: 2},
        {timestamp: "2023-03-03T20:43:00.000Z", id: 3, value: 2},
    ];
    stream.testInsert(...commitDocuments);

    // Wait for the two documents to be emitted.
    assert.soon(() => {
        jsTestLog(stream.stats());
        return stream.stats()['outputMessageCount'] == 2;
    });

    const metrics = sp.metrics();
    const stats = stream.stats(false /* verbose */);
    jsTestLog(stats);
    assert.eq(6, stats['inputMessageCount']);
    assert.eq(stats['inputMessageCount'],
              metrics['counters'].find((c) => c.name === 'num_input_documents').value);
    assert.gt(stats['inputMessageSize'], 0);
    assert.eq(stats['inputMessageSize'],
              metrics['counters'].find((c) => c.name === 'num_input_bytes').value);
    assert.eq(2, stats['outputMessageCount']);
    assert.eq(stats['outputMessageCount'],
              metrics['counters'].find((c) => c.name === 'num_output_documents').value);
    assert.gt(stats['outputMessageSize'], 0);
    assert.eq(stats['outputMessageSize'],
              metrics['counters'].find((c) => c.name === 'num_output_bytes').value);
    assert.gt(stats['stateSize'], 0);
    assert.eq(stats['watermark'], ISODate('2023-03-03T20:42:59.999Z'));
    assert.eq(stats['dlqMessageCount'],
              metrics['counters'].find((c) => c.name === 'num_dlq_documents').value);
    assert.eq(stats['dlqMessageSize'],
              metrics['counters'].find((c) => c.name === 'num_dlq_bytes').value);
    const verboseStats = stream.stats(true /* verbose */);
    jsTestLog(verboseStats);

    // Make sure that the summary stats emitted in `verbose` mode match
    // the summary stats emitted in the non-verbose mode.
    assert.eq(stats['inputMessageCount'], verboseStats['inputMessageCount']);
    assert.eq(stats['inputMessageSize'], verboseStats['inputMessageSize']);
    assert.eq(stats['outputMessageCount'], verboseStats['outputMessageCount']);
    assert.eq(stats['outputMessageSize'], verboseStats['outputMessageSize']);
    assert.eq(stats['stateSize'], verboseStats['stateSize']);

    const isExpectedWatermark = kafkaPartition => {
        // Needed to do this date conversion because ISODate comparisons weren't returning true for
        // equal values
        return new Date(kafkaPartition['watermark']).valueOf() ===
            (new Date(ISODate('2023-03-03T20:42:59.999Z')).valueOf());
    };

    // Make sure that you find the expected watermark in at least one of the partitions
    assert.eq(3, verboseStats['kafkaPartitions'].length);
    assert.eq(true, verboseStats['kafkaPartitions'].some(isExpectedWatermark));

    assert.eq(3, verboseStats['operatorStats'].length);

    // The source operator specific stats should match with the summary input stats.
    const sourceStats = verboseStats['operatorStats'][0];
    assert.eq('KafkaConsumerOperator', sourceStats['name']);
    assert.eq(verboseStats['inputMessageCount'], sourceStats['inputMessageCount']);
    assert.eq(verboseStats['inputMessageSize'], sourceStats['inputMessageSize']);
    assert.eq(0, sourceStats['dlqMessageCount']);
    assert.eq(0, sourceStats['dlqMessageSize']);
    assert.eq(0, sourceStats['stateSize']);

    // Input and output for the source operator should be the same since its
    // just funneling whatever it receives out to the next operator.
    assert.eq(sourceStats['inputMessageCount'], sourceStats['outputMessageCount']);

    const groupStats = verboseStats['operatorStats'][1];
    assert.eq('GroupOperator', groupStats['name']);
    assert.eq(6, groupStats['inputMessageCount']);
    assert.eq(2, groupStats['outputMessageCount']);

    // Group operator memory usage should align with the memory usage in
    // the stream summary stats, since the Group operator is the only stateful
    // operator.
    assert.eq(432, groupStats['stateSize']);

    // The sink operator specific stats should match with the summary output stats.
    const sinkStats = verboseStats['operatorStats'][2];
    assert.eq('InMemorySinkOperator', sinkStats['name']);
    assert.eq(verboseStats['outputMessageCount'], sinkStats['inputMessageCount']);
    assert.eq(verboseStats['outputMessageSize'], sinkStats['inputMessageSize']);
    assert.eq(2216, sinkStats['stateSize']);

    const totalStateSize =
        verboseStats["operatorStats"].reduce((sum, stats) => sum + stats["stateSize"], 0);
    assert.eq(totalStateSize, verboseStats['stateSize']);

    // Verify scaled stats match expected results.
    const scaleFactor = 10;
    const verboseScaledStats = stream.stats(true /* verbose */, scaleFactor);
    jsTestLog(verboseScaledStats);
    assert.eq(verboseScaledStats['inputMessageSize'],
              verboseStats['inputMessageSize'] / scaleFactor);
    assert.eq(verboseScaledStats['outputMessageSize'],
              verboseStats['outputMessageSize'] / scaleFactor);
    assert.eq(verboseScaledStats['stateSize'], verboseStats['stateSize'] / scaleFactor);
    assert.eq(verboseScaledStats['memoryTrackerBytes'],
              verboseStats['memoryTrackerBytes'] / scaleFactor);
    const sourceScaledStats = verboseScaledStats['operatorStats'][0];
    assert.eq(sourceScaledStats['inputMessageSize'], sourceStats['inputMessageSize'] / scaleFactor);
    const groupScaledStats = verboseScaledStats['operatorStats'][1];
    assert.eq(groupScaledStats['stateSize'], groupStats['stateSize'] / scaleFactor);
    const sinkScaledStats = verboseScaledStats['operatorStats'][2];
    assert.eq(sinkScaledStats['stateSize'], sinkStats['stateSize'] / scaleFactor);

    stream.stop();
})();

(function testGetStats_TimeSpent() {
    const sp = new Streams(TEST_TENANT_ID, [
        {name: 'db', type: 'atlas', options: {uri: `mongodb://${db.getMongo().host}`}},
        {
            name: 'kafka',
            type: 'kafka',
            options: {bootstrapServers: 'localhost:9092', isTestKafka: true}
        },
    ]);
    assert.commandWorked(db.adminCommand(
        {'configureFailPoint': 'matchOperatorSlowEventProcessing', 'mode': 'alwaysOn'}));
    const source = {
        $source: {
            connectionName: 'kafka',
            topic: 'topic',
            timeField: {$dateFromString: {'dateString': '$timestamp'}},
            testOnlyPartitionCount: NumberInt(1),
        },
    };
    const match = {$match: {"value": 1}};
    const stream = sp.createStreamProcessor('sp0', [source, match, sink.memory]);

    stream.start({featureFlags: {}});

    const documents = [
        // Two documents with same grouping key (`id`) and `timestamp`, so
        // they should fall into the same window and the same group within that
        // window, so only one document should be produced for the following
        // two input documents from the window operator.
        {timestamp: "2023-03-03T20:42:29.000Z", id: 1, value: 1},
        {timestamp: "2023-03-03T20:42:29.000Z", id: 1, value: 1},

        {timestamp: "2023-03-03T20:42:30.000Z", id: 2, value: 1},
    ];
    stream.testInsert(...documents);

    // Wait for the two documents to be emitted.
    assert.soon(() => {
        jsTestLog(stream.stats());
        return stream.stats()['outputMessageCount'] == 3;
    });
    const stats = stream.stats();
    jsTestLog(stats);
    assert.eq(3, stats['inputMessageCount']);
    const matchOperatorStats =
        stats['operatorStats'].filter((operatorStats) => operatorStats.name === "MatchOperator");
    assert.gte(matchOperatorStats[0].timeSpentMillis, 2500);
    stream.stop();

    assert.commandWorked(
        db.adminCommand({'configureFailPoint': 'matchOperatorSlowEventProcessing', 'mode': 'off'}));
})();

(function testGetStats_ExecutionTime() {
    const inputColl = db.input_coll;
    const outColl = db.output_coll;
    const dlqColl = db.dlq_coll;

    outColl.drop();
    dlqColl.drop();
    inputColl.drop();

    const uri = 'mongodb://' + db.getMongo().host;
    const sp = new Streams(TEST_TENANT_ID, [
        {name: 'db', type: 'atlas', options: {uri: uri}},
    ]);
    const pipeline = [
        {
            '$source': {
                'connectionName': 'db',
                'db': 'test',
                'coll': 'input_coll',
            }
        },
        {$replaceRoot: {newRoot: '$fullDocument'}},
        {$project: {i: {$range: [0, 10]}}},
        {$unwind: "$i"},
        {
            $project: {
                value: {
                    $range:
                        [{$multiply: ["$i", 1000000]}, {$multiply: [{$add: ["$i", 1]}, 1900000]}]
                }
            }
        },
        {$unwind: "$value"},
        {
            $tumblingWindow: {
                interval: {size: NumberInt(2), unit: 'second'},
                allowedLateness: NumberInt(0),
                idleTimeout: {size: NumberInt(1), unit: "second"},
                pipeline: [
                    {$group: {_id: null, count: {$sum: 1}}},
                ]
            }
        },
        {$emit: {connectionName: '__testMemory'}}
    ];

    const stream = sp.createStreamProcessor('sp0', pipeline);
    stream.start({featureFlags: {}});

    inputColl.insert({a: 1, b: 1});

    // Wait for executionTime of GroupOperator to be at least 1s.
    assert.soon(() => {
        const stats = stream.stats();
        jsTestLog(stats);
        const groupOperatorStats = stats['operatorStats'].filter(
            (operatorStats) => operatorStats.name === "GroupOperator");
        const unwindOperatorStats = stats['operatorStats'].filter(
            (operatorStats) => operatorStats.name === "UnwindOperator");
        const otherOperatorStats =
            stats['operatorStats'].filter((operatorStats) => operatorStats.name != "GroupOperator");
        if (groupOperatorStats.length === 1 && unwindOperatorStats.length == 2) {
            // TOOD(SERVER-97667): Enable these validations.
            // if (groupOperatorStats[0].executionTimeSecs < 1) {
            //     return false;
            // }
            // if (unwindOperatorStats[1].timeSpentMillis < 1) {
            //     return false;
            // }
            // for (let operatorStats of otherOperatorStats) {
            //     assert.lt(operatorStats.executionTimeSecs,
            //     groupOperatorStats[0].executionTimeSecs);
            // }
            assert(otherOperatorStats.length > 0);
            return true;
        }
        return false;
    });

    stream.stop();
})();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
}());
