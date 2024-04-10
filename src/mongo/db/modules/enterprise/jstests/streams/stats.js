/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {sink} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

(function() {

(function testGetStats_Basic() {
    const sp = new Streams([
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
    stream.start({});

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

    stream.stop();
})();

(function testGetStats_ExecutionTime() {
    const inputColl = db.input_coll;
    const outColl = db.output_coll;
    const dlqColl = db.dlq_coll;

    outColl.drop();
    dlqColl.drop();
    inputColl.drop();

    const uri = 'mongodb://' + db.getMongo().host;
    const sp = new Streams([
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
                        [{$multiply: ["$i", 1000000]}, {$multiply: [{$add: ["$i", 1]}, 1000000]}]
                }
            }
        },
        {$unwind: "$value"},
        {
            $tumblingWindow: {
                interval: {size: NumberInt(2), unit: 'second'},
                allowedLateness: {size: NumberInt(0), unit: 'second'},
                idleTimeout: {size: NumberInt(1), unit: "second"},
                pipeline: [
                    {$group: {_id: null, count: {$sum: 1}}},
                ]
            }
        },
        {$emit: {connectionName: '__testMemory'}}
    ];

    const stream = sp.createStreamProcessor('sp0', pipeline);
    stream.start({});

    inputColl.insert({a: 1, b: 1});

    // Wait for executionTime of GroupOperator to be at least 1s.
    assert.soon(() => {
        const stats = stream.stats();
        jsTestLog(stats);
        const groupOperatorStats = stats['operatorStats'].filter(
            (operatorStats) => operatorStats.name === "GroupOperator");
        const otherOperatorStats =
            stats['operatorStats'].filter((operatorStats) => operatorStats.name != "GroupOperator");
        if (groupOperatorStats.length === 1 && groupOperatorStats[0].executionTime >= 1) {
            assert(otherOperatorStats.length > 0);
            for (let operatorStats of otherOperatorStats) {
                assert.lt(operatorStats.executionTime, groupOperatorStats[0].executionTime);
            }
            return true;
        }
        return false;
    });

    stream.stop();
})();
}());
