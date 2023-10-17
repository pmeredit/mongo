/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {assertErrorCode} from "jstests/aggregation/extras/utils.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {sampleUntil, startSample} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

function createWindowOp(windowOp, interval, hopSize = null, pipeline = []) {
    let arg = {interval: interval, pipeline: pipeline};
    if (hopSize !== null && windowOp == "$hoppingWindow") {
        arg["hopSize"] = hopSize;
    }
    return {[windowOp]: arg};
}

function notAllowedFromAggPipeline(windowOp) {
    db.test1.insert({});
    let coll = db.test1;
    const interval = {size: NumberInt(1), unit: "second"};
    const groupPipeline = [{
        $group: {
            _id: "$id",
            sum: {$sum: "$value"},
        }
    }];
    assertErrorCode(coll, [createWindowOp(windowOp, interval, interval, groupPipeline)], 5491300);
}

function windowMergeSampleDLQ(windowOp) {
    const uri = 'mongodb://' + db.getMongo().host;
    let connectionRegistry = [
        {
            name: "kafka1",
            type: 'kafka',
            options: {bootstrapServers: 'localhost:9092', isTestKafka: true},
        },
        {name: "db1", type: 'atlas', options: {uri: uri}}
    ];
    const sp = new Streams(connectionRegistry);

    const interval = {size: NumberInt(1), unit: "second"};
    const groupPipeline = [
        {
            $group: {
                _id: "$id",
                sum: {$sum: "$value"},
            }
        },
        {$sort: {sum: 1}},
        {$limit: 1}
    ];

    sp.createStreamProcessor("window1", [
        {
            $source: {
                connectionName: "kafka1",
                topic: "test1",
                timeField: {$dateFromString: {"dateString": "$timestamp"}},
                allowedLateness: {size: NumberInt(0), unit: "second"},
                testOnlyPartitionCount: NumberInt(1)
            }
        },
        createWindowOp(windowOp, interval, interval, groupPipeline),
        {$merge: {into: {connectionName: "db1", db: "test", coll: "window1"}}}
    ]);

    // Start the streamProcessor.
    let result = sp.window1.start({dlq: {connectionName: "db1", db: "test", coll: "dlq1"}});
    assert.commandWorked(result);

    function insert(docs) {
        // Insert 2 documents into the stream.
        let insertCmd = {streams_testOnlyInsert: '', name: "window1", documents: docs};
        let result = db.runCommand(insertCmd);
        assert.commandWorked(result);
    }

    // Start a sample on the stream processor.
    let cursorId = startSample("window1");

    // Insert a few docs into the stream processor
    let docs = [
        {timestamp: "2023-03-03T20:42:30.000Z", id: 0, value: 1},
        {timestamp: "2023-03-03T20:42:31.000Z", id: 1, value: 1},
        {timestamp: "2023-03-03T20:42:32.000Z", id: 2, value: 1},
        {timestamp: "2023-03-03T20:42:31.000Z", id: 3, value: 1},  // Late!
        {timestamp: "2023-03-03T20:42:30.000Z", id: 3, value: 1},  // Late!
        {timestamp: "2023-03-03T20:42:29.000Z", id: 3, value: 1},  // Late!
        {timestamp: "2023-03-03T20:42:33.000Z", id: 4, value: 1},
        {timestamp: "2023-03-03T20:42:34.001Z", id: 5, value: 1},
    ];
    insert(docs);

    // Validate we see the expected number of windows in the sample.
    const expectedWindowCount = 4;
    sampleUntil(cursorId, expectedWindowCount, "window1");
    // Validate we see the expected number of windows in the $merge collection.
    // let windowResults = db.getSiblingDB("test").window1.find({});
    // assert.eq(expectedWindowCount, windowResults.length());

    assert.soon(
        () => { return db.getSiblingDB("test").window1.find({}).length() == expectedWindowCount; });

    // Validate there are 3 late events in the DLQ
    let dlqResults = db.getSiblingDB("test").dlq1.find({});
    assert.eq(3, dlqResults.length());

    // Stop the streamProcessor.
    result = sp.window1.stop();
    assert.commandWorked(result);
}

for (const windowOp of ["$tumblingWindow", "$hoppingWindow"]) {
    jsTestLog("Testing op " + windowOp);

    // Clear our db before testing a new op.
    db.dropDatabase();
    notAllowedFromAggPipeline(windowOp);
    windowMergeSampleDLQ(windowOp);
}

/**
 * End to end test for $hoppingWindow.
 */
(function testHoppingWindowOverlappingWindows() {
    db.dropDatabase();
    const uri = 'mongodb://' + db.getMongo().host;
    let connectionRegistry = [
        {
            name: "kafka1",
            type: 'kafka',
            options: {bootstrapServers: 'localhost:9092', isTestKafka: true},
        },
        {name: "db1", type: 'atlas', options: {uri: uri}}
    ];
    const sp = new Streams(connectionRegistry);

    sp.createStreamProcessor("window1", [
        {
            $source: {
                connectionName: "kafka1",
                topic: "test1",
                timeField: {$dateFromString: {"dateString": "$timestamp"}},
                allowedLateness: {size: NumberInt(0), unit: "second"},
                testOnlyPartitionCount: NumberInt(1)
            }
        },
        {
            $hoppingWindow: {
                interval: {size: NumberInt(5), unit: "second"},
                hopSize: {size: NumberInt(2), unit: "second"},
                pipeline: [
                    {
                        $group: {
                            _id: "$id",
                            sum: {$sum: "$value"},
                        }
                    },
                    {$sort: {sum: -1}},
                    {$limit: 1},
                ]
            }
        },
        {$merge: {into: {connectionName: "db1", db: "test", coll: "window1"}}}
    ]);

    // Start the streamProcessor.
    let result = sp.window1.start({dlq: {connectionName: "db1", db: "test", coll: "dlq1"}});
    assert.commandWorked(result);

    function insert(docs) {
        // Insert 'docs' into the stream.
        let insertCmd = {streams_testOnlyInsert: '', name: "window1", documents: docs};
        let result = db.runCommand(insertCmd);
        assert.commandWorked(result);
    }

    // Start a sample on the stream processor.
    let cursorId = startSample("window1");

    // Insert a few docs into the stream processor
    let docs = [
        {timestamp: "2023-03-03T20:42:29.000Z", id: 0, value: 1},
        {timestamp: "2023-03-03T20:42:30.000Z", id: 0, value: 1},
        {timestamp: "2023-03-03T20:42:31.100Z", id: 1, value: 1},
        {timestamp: "2023-03-03T20:42:31.200Z", id: 1, value: 1},
        {timestamp: "2023-03-03T20:42:31.300Z", id: 1, value: 1},
        {timestamp: "2023-03-03T20:42:31.400Z", id: 1, value: 1},
        {timestamp: "2023-03-03T20:42:32.000Z", id: 2, value: 6},
        {timestamp: "2023-03-03T20:42:33.000Z", id: 4, value: 1},
        {timestamp: "2023-03-03T20:42:34.100Z", id: 5, value: 1},
        {timestamp: "2023-03-03T20:42:35.100Z", id: 5, value: 7},
        {timestamp: "2023-03-03T20:42:37.100Z", id: 2, value: 1},
        {timestamp: "2023-03-03T20:42:37.100Z", id: 6, value: 10},
        {timestamp: "2023-03-03T20:42:38.100Z", id: 5, value: 1},
        {timestamp: "2023-03-03T20:42:39.100Z", id: 7, value: 20},
        {timestamp: "2023-03-03T20:42:41.200Z", id: 8, value: 25},
        {
            timestamp: "2023-03-03T20:42:44.000Z",
            id: 11,
            value: 30
        },  // This will force other windows to close.
    ];
    insert(docs);

    // Validate we see the expected number of windows in the sample.
    const expectedWindowCount = 7;
    sampleUntil(cursorId, expectedWindowCount, "window1");

    // Validate we see the expected number of windows in the $merge collection, and that the
    // contents match.
    var windowResults;
    assert.soon(() => {
        windowResults = db.getSiblingDB("test").window1.find({}, {_stream_meta: 0}).toArray();
        return windowResults.length == expectedWindowCount;
    });

    // window -> groups -> greatest value
    // [25,30) -> (0, 2) -> (0, 2) is the greatest
    // [27,32) -> (0, 2), (1, 4) -> (1, 4) is the greatest
    // [29,34) -> (0, 2), (1, 4), (2, 6), (4, 1) -> (2, 6) is the greatest
    // [31,36) -> (1, 4), (2, 6), (4, 1), (5, 8) -> (5, 8) is the greatest
    // [33,38) -> (2, 6), (4, 1), (5, 8), (6, 10) -> (6, 10) is the greatest
    // [35,40) -> (2, 1), (5, 8), (6, 10), (7, 20) -> (7, 20) is the greatest
    // [37,42) -> (2, 1), (5,1), (6, 10), (7, 20), (8, 25) -> (8, 25) is the greatest
    const expectedResults = [
        {"_id": 0, "sum": 1},
        {"_id": 1, "sum": 4},
        {"_id": 2, "sum": 6},
        {"_id": 5, "sum": 8},
        {"_id": 6, "sum": 10},
        {"_id": 7, "sum": 20},
        {"_id": 8, "sum": 25}
    ];
    assert.eq(windowResults, expectedResults);

    // Stop the streamProcessor.
    result = sp.window1.stop();
    assert.commandWorked(result);
}());
