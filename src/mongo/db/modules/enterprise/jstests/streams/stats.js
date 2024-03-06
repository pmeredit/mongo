/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {sink} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

(function testStream_GetStats() {
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
            allowedLateness: {size: NumberInt(1), unit: 'second'},
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
    stream.start();

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
    assert.eq(0, sourceStats['stateSize']);

    // Input and output for the source operator should be the same since its
    // just funneling whatever it receives out to the next operator.
    assert.eq(sourceStats['inputMessageCount'], sourceStats['outputMessageCount']);

    const windowStats = verboseStats['operatorStats'][1];
    assert.eq('WindowOperator', windowStats['name']);
    assert.eq(4, windowStats['inputMessageCount']);
    assert.eq(2, windowStats['outputMessageCount']);

    // Window operator memory usage should align with the memory usage in
    // the stream summary stats, since the window operator is the only stateful
    // operator.
    assert.eq(16, windowStats['stateSize']);

    // The sink operator specific stats should match with the summary output stats.
    const sinkStats = verboseStats['operatorStats'][2];
    assert.eq('InMemorySinkOperator', sinkStats['name']);
    assert.eq(verboseStats['outputMessageCount'], sinkStats['inputMessageCount']);
    assert.eq(verboseStats['outputMessageSize'], sinkStats['inputMessageSize']);
    assert.eq(1140, sinkStats['stateSize']);

    assert.eq(verboseStats['stateSize'], windowStats['stateSize'] + sinkStats['stateSize']);

    stream.stop();
})();
