/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

// TODO(SERVER-93180): Finish these tests

import {assertErrorCode} from "jstests/aggregation/extras/utils.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    getStats,
    insertDocs,
    listStreamProcessors,
    sampleUntil,
    startSample,
    TEST_TENANT_ID,
    verifyInputEqualsOutput
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

const sortOp = {
    $sort: {"id": 1, "a": 1},
};

const groupOp = {
    $group: {_id: null, allIds: {$push: "$id"}}
};

const matchOp = {
    $match: {b: 1},
};

const limitOp = {
    $limit: 1000
};

const sortWindowIdOp = {
    $sort: {"_stream_meta.window.partition": 1}
};

const addFieldWindowIdOp = {
    $addFields: {x: "$_stream_meta.window.start"}
};

const noBlockingWindowAwarePipeline = [matchOp, {$addFields: {allIds: ["$id"]}}];
const streamMetaDepBeforeFirstBlockingPipeline = [addFieldWindowIdOp, matchOp, sortOp, groupOp];
// extra sort to undo 1st sort
const streamMetaDepInFirstBlockingPipeline = [sortWindowIdOp, groupOp];
const streamMetaDepAfterFirstBlockingPipeline = [groupOp, sortWindowIdOp];
// limit is super big so doesn't actually limit anything for these tests
const limitBeforeFirstBlockingPipeline = [limitOp, sortOp, groupOp];
const limitAfterFirstBlockingPipeline = [sortOp, limitOp, groupOp];
const firstOperatorWindowAwarePipeline = [sortOp, groupOp];
const firstOperatorNOTWindowAwarePipeline = [matchOp, sortOp, groupOp];

const validPipelines = [
    firstOperatorWindowAwarePipeline,
    firstOperatorNOTWindowAwarePipeline,
    limitAfterFirstBlockingPipeline,
    streamMetaDepAfterFirstBlockingPipeline
];

const invalidPipelines = [
    noBlockingWindowAwarePipeline,
    streamMetaDepBeforeFirstBlockingPipeline,
    streamMetaDepInFirstBlockingPipeline,
    limitBeforeFirstBlockingPipeline
];

function createSessionWindow(pipeline, gap, partitionBy, allowedLateness) {
    let arg = {
        pipeline: pipeline,
        gap: gap,
        partitionBy: partitionBy,
        allowedLateness: allowedLateness ? allowedLateness : NumberInt(0)
    };
    return {["$sessionWindow"]: arg};
}

function testRunner(
    pipeline, gap, partitionBy, dataMsg, expectedWindows, shouldFail, allowedLateness) {
    const uri = 'mongodb://' + db.getMongo().host;
    let connectionRegistry = [
        {
            name: "kafka1",
            type: 'kafka',
            options: {bootstrapServers: 'localhost:9092', isTestKafka: true},
        },
        {name: "db1", type: 'atlas', options: {uri: uri}}
    ];
    const sp = new Streams(TEST_TENANT_ID, connectionRegistry);

    db.dropDatabase();

    const cmd = [
        {
            $source: {
                connectionName: "kafka1",
                topic: "test",
                timeField: {$dateFromString: {"dateString": "$ts"}},
                testOnlyPartitionCount: NumberInt(1)
            }
        },
        createSessionWindow(pipeline, gap, partitionBy, allowedLateness),
        {$project: {_id: 0}},
        {$merge: {into: {connectionName: "db1", db: "test", coll: "sessionWindows"}}}
    ];

    sp.createStreamProcessor("sessionWindows", cmd);

    // Start the streamProcessor.
    if (shouldFail) {
        const streamProcessorInvalidOptions = 420;
        assert.throwsWithCode(
            () => sp.sessionWindows.start({featureFlags: {enableSessionWindow: true}}),
            streamProcessorInvalidOptions);
        return;
    }
    let result = sp.sessionWindows.start({featureFlags: {enableSessionWindow: true}});
    assert.commandWorked(result);

    let expectedMsgCount = 0;
    for (const docs of dataMsg) {
        insertDocs("sessionWindows", docs);
        expectedMsgCount += docs.length;
        assert.soon(() => {
            const stats = sp.sessionWindows.stats();
            return stats.inputMessageCount == expectedMsgCount;
        });
    }

    // Validate we see the expected number of windows in the $merge collection, and that the
    // contents match.
    var windowResults;
    assert.soon(() => {
        const stats = sp.sessionWindows.stats();
        jsTestLog(stats);
        windowResults = db.getSiblingDB("test").sessionWindows.find().toArray();
        return windowResults.length == expectedWindows.length;
    });

    // Stop the streamProcessor.
    result = sp.sessionWindows.stop();
    assert.commandWorked(result);
    for (let doc of windowResults) {
        delete doc._stream_meta.source;
    }
    verifyInputEqualsOutput(windowResults, expectedWindows, ["_id", "_ts"]);
    return windowResults;
}

function getTime(hours, minutes, seconds, millis) {
    return new Date(hours * 1000 * 3600 + minutes * 1000 * 60 + seconds * 1000 + millis)
        .toISOString();
}

function buildExpectedDoc({allIds, start, end, partition}) {
    return {
        "allIds": allIds,
        "_stream_meta": {
            "window": {
                "start": start,
                "end": end,
                "partition": partition

            }
        }
    };
}

function invalidPipelinesShouldError() {
    const gap = {size: NumberInt(1), unit: "hour"};
    const partitionBy = "$a";

    let docs1 = [
        {a: 1, b: 1, id: 0, ts: getTime(0, 0, 0, 0)},
    ];

    for (const pipeline of invalidPipelines) {
        testRunner(pipeline, gap, partitionBy, [docs1], [], true);
    }
}

function noMerge() {
    const gap = {size: NumberInt(1), unit: "hour"};
    const partitionBy = "$a";

    let docs1 = [
        {a: 1, b: 1, id: 0, ts: getTime(0, 0, 0, 0)},
        {a: 1, b: 1, id: 1, ts: getTime(0, 40, 0, 0)}
    ];

    let docs2 = [{a: 5, b: 1, id: 2, ts: getTime(1, 40, 0, 1)}];

    let expectedOutput = [buildExpectedDoc({
        allIds: [0, 1],
        start: ISODate(getTime(0, 0, 0, 0)),
        end: ISODate(getTime(1, 40, 0, 0), 1),
        partition: 1
    })];

    for (const pipeline of validPipelines) {
        testRunner(pipeline, gap, partitionBy, [docs1, docs2], expectedOutput, false);
    }
}

function mergeMultiple() {
    const gapSeconds = 200;
    const gap = {size: NumberInt(gapSeconds), unit: "second"};
    const partitionBy = "$a";

    let docs = [];
    let N = 10;
    for (let i = 0; i < N; i++) {
        docs.push([{a: 1, b: 1, id: i, ts: getTime(0, 0, i, 0)}]);
    }

    docs.push([{a: 1, b: 1, id: N, ts: getTime(0, 0, 400, 0)}]);

    let expectedOutput = [buildExpectedDoc({
        allIds: Array.from({length: N}, (_, i) => i),
        start: ISODate(getTime(0, 0, 0, 0)),
        end: ISODate(getTime(0, 0, N - 1 + gapSeconds, 0)),
        partition: 1
    })];

    for (const pipeline of validPipelines) {
        testRunner(pipeline, gap, partitionBy, docs, expectedOutput, false);
    }
}

function mergeMultipleWithManyPartitions() {
    const gapSeconds = 200;
    const gap = {size: NumberInt(gapSeconds), unit: "second"};
    const partitionBy = "$a";

    let docs = [];
    let numPartitions = 5;
    let N = 2;
    for (let p = 0; p < numPartitions; p++) {
        for (let i = 0; i < N; i++) {
            docs.push([{a: p, b: 1, id: i + p * N, ts: getTime(0, 0, i, 0)}]);
        }
    }

    docs.push([{a: 1, b: 1, id: N, ts: getTime(0, 0, 400, 0)}]);

    let expectedOutput = [];
    for (let p = 0; p < numPartitions; p++) {
        expectedOutput.push(buildExpectedDoc({
            allIds: Array.from({length: N}, (_, i) => i + p * N),
            start: ISODate(getTime(0, 0, 0, 0)),
            end: ISODate(getTime(0, 0, N - 1 + gapSeconds, 0)),
            partition: p
        }));
    }

    for (const pipeline of validPipelines) {
        testRunner(pipeline, gap, partitionBy, docs, expectedOutput, false, {
            size: NumberInt(10),
            unit: "second"
        });
    }
}

invalidPipelinesShouldError();
noMerge();
mergeMultiple();
mergeMultipleWithManyPartitions();
