/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {sink, source} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

(function testStream_GetStats() {
    const sp = new Streams([
        {name: 'db', type: 'atlas', options: {uri: `mongodb://${db.getMongo().host}`}},
    ]);
    const match = {$match: {value: 1}};
    const stream = sp.createStreamProcessor('sp0', [source.memory, match, sink.memory]);
    stream.start();

    const documents = [
        {timestamp: "2023-03-03T20:42:29.000Z", id: 1, value: 1},
        {timestamp: "2023-03-03T20:42:30.000Z", id: 1, value: 1},
        {timestamp: "2023-03-03T20:42:30.000Z", id: 2, value: 2},
    ];
    stream.testInsert(...documents);

    // Wait for the two documents to be emitted.
    assert.soon(() => stream.stats()['outputDocs'] == 2);

    const stats = stream.stats(false /* verbose */);
    jsTestLog(stats);
    assert.eq(3, stats['inputDocs']);
    assert.gt(stats['inputBytes'], 0);
    assert.eq(2, stats['outputDocs']);
    assert.gt(stats['outputBytes'], 0);

    const verboseStats = stream.stats(true /* verbose */);
    jsTestLog(verboseStats);

    // Make sure that the summary stats emitted in `verbose` mode match
    // the summary stats emitted in the non-verbose mode.
    assert.eq(stats['inputDocs'], verboseStats['inputDocs']);
    assert.eq(stats['inputBytes'], verboseStats['inputBytes']);
    assert.eq(stats['outputDocs'], verboseStats['outputDocs']);
    assert.eq(stats['outputBytes'], verboseStats['outputBytes']);
    assert.eq(3, verboseStats['operatorStats'].length);

    // The source operator specific stats should match with the summary input stats.
    const sourceStats = verboseStats['operatorStats'][0];
    assert.eq('InMemorySourceOperator', sourceStats['name']);
    assert.eq(verboseStats['inputDocs'], sourceStats['inputDocs']);
    assert.eq(verboseStats['inputBytes'], sourceStats['inputBytes']);
    assert.eq(0, sourceStats['dlqDocs']);

    // Input and output for the source operator should be the same since its
    // just funneling whatever it receives out to the next operator.
    assert.eq(sourceStats['inputDocs'], sourceStats['outputDocs']);

    const matchStats = verboseStats['operatorStats'][1];
    assert.eq('MatchOperator', matchStats['name']);
    assert.eq(3, matchStats['inputDocs']);
    assert.eq(2, matchStats['outputDocs']);

    // The sink operator specific stats should match with the summary output stats.
    const sinkStats = verboseStats['operatorStats'][2];
    assert.eq('InMemorySinkOperator', sinkStats['name']);
    assert.eq(verboseStats['outputDocs'], sinkStats['inputDocs']);
    assert.eq(verboseStats['outputBytes'], sinkStats['inputBytes']);

    stream.stop();
})();
