/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // For assertErrorCode().
load('src/mongo/db/modules/enterprise/jstests/streams/fake_client.js');
load('src/mongo/db/modules/enterprise/jstests/streams/utils.js');

function notAllowedFromAggPipeline() {
    db.test1.insert({});
    let coll = db.test1;
    assertErrorCode(coll,
                    [{
                        $tumblingWindow: {
                            interval: {size: 1, unit: "second"},
                            pipeline: [
                                {
                                    $group: {
                                        _id: "$id",
                                        sum: {$sum: "$value"},
                                    }
                                },
                            ]
                        }
                    }],
                    5491300);
}

function windowMergeSampleDLQ() {
    const uri = 'mongodb://' + db.getMongo().host;
    let connectionRegistry = [
        {
            name: "kafka1",
            type: 'kafka',
            options: {bootstrapServers: 'localhost:9092', isTestKafka: true},
        },
        {name: "db1", type: 'atlas', options: {uri: uri}}
    ];
    sp = new Streams(connectionRegistry);

    sp.createStreamProcessor("window1", [
        {
            $source: {
                connectionName: "kafka1",
                topic: "test1",
                timeField: {$dateFromString: {"dateString": "$timestamp"}},
                allowedLateness: {size: NumberInt(0), unit: "second"},
                partitionCount: NumberInt(1)
            }
        },
        {
            $tumblingWindow: {
                interval: {size: NumberInt(1), unit: "second"},
                pipeline: [
                    {$sort: {value: 1}},
                    {
                        $group: {
                            _id: "$id",
                            sum: {$sum: "$value"},
                        }
                    },
                    {$sort: {sum: 1}},
                    {$limit: 1}
                ]
            }
        },
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
    let windowResults = db.getSiblingDB("test").window1.find({});
    assert.eq(expectedWindowCount, windowResults.length());

    // Validate there are 3 late events in the DLQ
    let dlqResults = db.getSiblingDB("test").dlq1.find({});
    assert.eq(3, dlqResults.length());

    // Stop the streamProcessor.
    result = sp.window1.stop();
    assert.commandWorked(result);
}

notAllowedFromAggPipeline();
windowMergeSampleDLQ();
}());