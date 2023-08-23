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
            partitionCount: NumberInt(1),
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
        return stream.stats()['outputDocs'] == 2;
    });

    const stats = stream.stats(false /* verbose */);
    jsTestLog(stats);
    assert.eq(4, stats['inputDocs']);
    assert.gt(stats['inputBytes'], 0);
    assert.eq(2, stats['outputDocs']);
    assert.gt(stats['outputBytes'], 0);
    assert.gt(stats['stateSize'], 0);

    const verboseStats = stream.stats(true /* verbose */);
    jsTestLog(verboseStats);

    // Make sure that the summary stats emitted in `verbose` mode match
    // the summary stats emitted in the non-verbose mode.
    assert.eq(stats['inputDocs'], verboseStats['inputDocs']);
    assert.eq(stats['inputBytes'], verboseStats['inputBytes']);
    assert.eq(stats['outputDocs'], verboseStats['outputDocs']);
    assert.eq(stats['outputBytes'], verboseStats['outputBytes']);
    assert.eq(stats['stateSize'], verboseStats['stateSize']);

    assert.eq(3, verboseStats['operatorStats'].length);

    // The source operator specific stats should match with the summary input stats.
    const sourceStats = verboseStats['operatorStats'][0];
    assert.eq('KafkaConsumerOperator', sourceStats['name']);
    assert.eq(verboseStats['inputDocs'], sourceStats['inputDocs']);
    assert.eq(verboseStats['inputBytes'], sourceStats['inputBytes']);
    assert.eq(0, sourceStats['dlqDocs']);
    assert.eq(0, sourceStats['stateSize']);

    // Input and output for the source operator should be the same since its
    // just funneling whatever it receives out to the next operator.
    assert.eq(sourceStats['inputDocs'], sourceStats['outputDocs']);

    const windowStats = verboseStats['operatorStats'][1];
    assert.eq('WindowOperator', windowStats['name']);
    assert.eq(4, windowStats['inputDocs']);
    assert.eq(2, windowStats['outputDocs']);

    // Window operator memory usage should align with the memory usage in
    // the stream summary stats, since the window operator is the only stateful
    // operator.
    assert.eq(16, windowStats['stateSize']);

    // The sink operator specific stats should match with the summary output stats.
    const sinkStats = verboseStats['operatorStats'][2];
    assert.eq('InMemorySinkOperator', sinkStats['name']);
    assert.eq(verboseStats['outputDocs'], sinkStats['inputDocs']);
    assert.eq(verboseStats['outputBytes'], sinkStats['inputBytes']);
    assert.eq(1140, sinkStats['stateSize']);

    assert.eq(verboseStats['stateSize'], windowStats['stateSize'] + sinkStats['stateSize']);

    stream.stop();
})();
