/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {assertErrorCode} from "jstests/aggregation/extras/utils.js";
import {
    commonTestSetup,
    Streams,
    test
} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    listStreamProcessors,
    sampleUntil,
    startSample,
    TEST_TENANT_ID,
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

function runAll() {
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

    function createWindowOp(windowOp, interval, hopSize = null, pipeline = []) {
        let arg = {interval: interval, allowedLateness: NumberInt(0), pipeline: pipeline};
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
        assertErrorCode(
            coll, [createWindowOp(windowOp, interval, interval, groupPipeline)], 5491300);
    }

    function windowMergeSampleDLQ(windowOp) {
        const uri = 'mongodb://' + db.getMongo().host;

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
                    testOnlyPartitionCount: NumberInt(1)
                }
            },
            createWindowOp(windowOp, interval, interval, groupPipeline),
            {$merge: {into: {connectionName: "db1", db: "test", coll: "window1"}}}
        ]);

        // Start the streamProcessor.
        let result = sp.window1.start(
            {dlq: {connectionName: "db1", db: "test", coll: "dlq1"}, featureFlags: {}});
        assert.commandWorked(result);

        function insert(docs) {
            // Insert 2 documents into the stream.
            let insertCmd = {
                streams_testOnlyInsert: '',
                tenantId: TEST_TENANT_ID,
                name: "window1",
                documents: docs
            };
            let result = db.runCommand(insertCmd);
            assert.commandWorked(result);
        }

        // Start a sample on the stream processor.
        let cursorId = startSample("window1")["id"];

        // Insert a few docs into the stream processor
        let docs = [
            {timestamp: "2023-03-03T20:42:30.000Z", id: 0, value: 1},
            {timestamp: "2023-03-03T20:42:31.000Z", id: 1, value: 1},
            {timestamp: "2023-03-03T20:42:32.000Z", id: 2, value: 1},
            {timestamp: "2023-03-03T20:42:31.000Z", id: 3, value: 1},  // Late, but accepted!
            {timestamp: "2023-03-03T20:42:30.000Z", id: 4, value: 1},  // Late, but accepted!
            {timestamp: "2023-03-03T20:42:29.000Z", id: 5, value: 1},  // Late, but accepted!
            {timestamp: "2023-03-03T20:42:33.000Z", id: 6, value: 1},
            {timestamp: "2023-03-03T20:42:34.001Z", id: 7, value: 1},
        ];
        insert(docs);

        // Validate we see the expected number of windows in the sample.
        const expectedWindowCount = 5;
        sampleUntil(cursorId, expectedWindowCount, "window1");

        // Validate we see the expected number of windows in the $merge collection.
        assert.soon(() => {
            return db.getSiblingDB("test").window1.find({}).length() == expectedWindowCount;
        });

        // No documents are rejected, because there is only one watermark event sent to window
        // stage. which is processed after the current batch is processed.
        let dlqResults = db.getSiblingDB("test").dlq1.find({});
        assert.eq(0, dlqResults.length());

        // Previous windows are closed and sending new documents for those windows should result in
        // dlq messages
        let lateDocs = [
            {timestamp: "2023-03-03T20:42:28.000Z", id: 8, value: 1},   // Late
            {timestamp: "2023-03-03T20:42:28.000Z", id: 9, value: 1},   // Late
            {timestamp: "2023-03-03T20:42:30.000Z", id: 10, value: 1},  // Late
        ];
        insert(lateDocs);
        // Validate there are 3 late events in the DLQ
        assert.soon(
            () => { return db.getSiblingDB("test").dlq1.find({}).length() == lateDocs.length; });

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
        sp.createStreamProcessor("window1", [
            {
                $source: {
                    connectionName: "kafka1",
                    topic: "test1",
                    timeField: {$dateFromString: {"dateString": "$timestamp"}},
                    testOnlyPartitionCount: NumberInt(1)
                }
            },
            {
                $hoppingWindow: {
                    interval: {size: NumberInt(5), unit: "second"},
                    allowedLateness: NumberInt(0),
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
        let result = sp.window1.start(
            {dlq: {connectionName: "db1", db: "test", coll: "dlq1"}, featureFlags: {}});
        assert.commandWorked(result);

        function insert(docs) {
            // Insert 'docs' into the stream.
            let insertCmd = {
                streams_testOnlyInsert: '',
                tenantId: TEST_TENANT_ID,
                name: "window1",
                documents: docs
            };
            let result = db.runCommand(insertCmd);
            assert.commandWorked(result);
        }

        // Start a sample on the stream processor.
        let cursorId = startSample("window1")["id"];

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

    /**
     * End to end test for window idle timeout.
     */
    (function testWindowIdleTimeout() {
        const kafkaSourceType = "kafka";
        const changestreamSourceType = "changestream";
        const innerTest = (sourceType) => {
            db.dropDatabase();

            const dbName = "test";
            const inputCollName = "testinput";
            const inputColl = db.getSiblingDB(dbName)[inputCollName];
            const outputCollName = "testoutput";
            const outputColl = db.getSiblingDB(dbName)[outputCollName];
            const uri = 'mongodb://' + db.getMongo().host;
            const spName = "idlewindows";
            let connectionRegistry = [
                {name: "db1", type: 'atlas', options: {uri: uri}},
                {
                    name: "kafka1",
                    type: 'kafka',
                    options: {bootstrapServers: 'localhost:9092', isTestKafka: true},
                },
            ];
            let source = {};
            let isKafka = sourceType == kafkaSourceType;
            if (isKafka) {
                source = {
                    $source: {
                        connectionName: "kafka1",
                        topic: "test1",
                        testOnlyPartitionCount: NumberInt(1)
                    }
                };
            } else {
                source = {$source: {connectionName: "db1", db: dbName, coll: inputCollName}};
            }

            const sp = new Streams(TEST_TENANT_ID, connectionRegistry);
            const hopSizeSeconds = 1;
            sp.createStreamProcessor(spName, [
                source,
                {
                    $hoppingWindow: {
                        interval: {size: NumberInt(5), unit: "second"},
                        hopSize: {size: NumberInt(hopSizeSeconds), unit: "second"},
                        pipeline: [{
                            $group: {
                                _id: null,
                                count: {$count: {}},
                            }
                        }],
                        idleTimeout: {size: NumberInt(3), unit: "second"}
                    }
                },
                {$project: {_id: 0}},
                {$merge: {into: {connectionName: "db1", db: dbName, coll: outputCollName}}}
            ]);

            // Start the streamProcessor.
            let result = sp[spName].start();
            assert.commandWorked(result);

            function insert(doc) {
                if (isKafka) {
                    // Insert 'docs' into the stream.
                    assert.commandWorked(db.runCommand({
                        streams_testOnlyInsert: '',
                        tenantId: TEST_TENANT_ID,
                        name: spName,
                        documents: [doc]
                    }));
                } else {
                    assert.commandWorked(inputColl.insertOne(doc));
                }
            }

            // Insert a couple docs into the stream processor.
            insert({id: 1, value: 1});
            // Sleep for the twice the hop size of the window.
            sleep(2 * hopSizeSeconds * 1000);
            insert({id: 2, value: 2});
            // The first doc will always belong in 5 windows.
            // The the second doc will open one or more new windows.
            const minimumExpectedWindowCount = 5 + 1;
            // The idle timeout will eventually occur and the we should see the expected windows.
            assert.soon(() => {
                let results = outputColl.find({}).sort({"_stream_meta.windowStart": 1}).toArray();
                return results.length >= minimumExpectedWindowCount;
            });

            // Stop the streamProcessor.
            assert.commandWorked(sp[spName].stop());
        };

        innerTest(kafkaSourceType);
        innerTest(changestreamSourceType);
    }());

    /**
     * This test is for small windows (200ms) with a small idleTimeout (200ms).
     * In SERVER-93117 we realized a window like could cause some documents to be DLQ-ed due to
     * lateness.
     */
    (function testWindowSmallIdleTimeout() {
        // Start a streamProcessor.
        const [sp, inputColl, outputColl] = commonTestSetup();
        const spName = "windowSmallIdleTimeout";
        sp.createStreamProcessor(spName, [
            {
                $source: {
                    connectionName: test.atlasConnection,
                    db: test.dbName,
                    coll: test.inputCollName
                }
            },
            {
                $tumblingWindow: {
                    interval: {unit: "ms", size: NumberInt(200)},
                    idleTimeout: {unit: "ms", size: NumberInt(200)},
                    allowedLateness: {unit: "second", size: NumberInt(0)},
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}]
                }
            },
            {$project: {_id: 0}},
            {
                $merge: {
                    into: {
                        connectionName: test.atlasConnection,
                        db: test.dbName,
                        coll: test.outputCollName
                    }
                }
            },
        ]);
        sp[spName].start();

        // Insert 100 documents, sleeping for 2 seconds in between a few of them.
        const numDocs = 100;
        for (let i = 0; i < numDocs; i++) {
            inputColl.insertOne({a: i});
            if (i % 25 == 0) {
                sleep(2000);
            }
        }

        // Wait for all messages to be processoed.
        assert.soon(() => {
            const stats = sp[spName].stats();
            return numDocs == stats.inputMessageCount;
        });
        const stats = sp[spName].stats();
        // Validate no messages are DLQ-ed.
        assert.eq(0, stats.dlqMessageCount);

        // Stop the streamProcessor.
        sp[spName].stop();
        assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
    }());
}

function testWindowMinMaxStats() {
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
    const sp = new Streams(TEST_TENANT_ID, connectionRegistry);
    sp.createStreamProcessor("window1", [
        {
            $source: {
                connectionName: "kafka1",
                topic: "test1",
                timeField: {$dateFromString: {"dateString": "$timestamp"}},
                testOnlyPartitionCount: NumberInt(1)
            }
        },
        {
            $hoppingWindow: {
                interval: {size: NumberInt(5), unit: "second"},
                allowedLateness: NumberInt(0),
                hopSize: {size: NumberInt(1), unit: "second"},
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
    let result = sp.window1.start(
        {dlq: {connectionName: "db1", db: "test", coll: "dlq1"}, featureFlags: {}});
    assert.commandWorked(result);

    function insert(docs) {
        // Insert 'docs' into the stream.
        let insertCmd = {
            streams_testOnlyInsert: '',
            tenantId: TEST_TENANT_ID,
            name: "window1",
            documents: docs
        };
        let result = db.runCommand(insertCmd);
        assert.commandWorked(result);
    }

    insert([
        // belongs to 26-31 window through 30-35 window
        {timestamp: "2023-03-03T20:42:30.000Z", id: 0, value: 1},
    ]);
    assert.soon(() => {
        const stats = sp.window1.stats();
        const windowStats = stats.operatorStats[1];
        const expectedWatermark = ISODate('2023-03-03T20:42:29.999Z');
        const expectedMin = ISODate('2023-03-03T20:42:26Z');
        const expectedMax = ISODate('2023-03-03T20:42:30Z');
        const actualMin = windowStats["minOpenWindowStartTime"];
        const actualMax = windowStats["maxOpenWindowStartTime"];
        const actualWatermark = stats["watermark"];
        try {
            assert.eq(expectedMin, actualMin);
            assert.eq(expectedMax, actualMax);
            assert.eq(expectedWatermark, actualWatermark);
            return true;
        } catch (e) {
            return false;
        }
    });

    insert([
        // belongs to 28-33 window through 32-37 window
        // will send watermark at 31.999, closing 26-31, leaving open 27-32
        {timestamp: "2023-03-03T20:42:32.000Z", id: 2, value: 6},
    ]);
    assert.soon(() => {
        const stats = sp.window1.stats();
        const windowStats = stats.operatorStats[1];
        const expectedWatermark = ISODate('2023-03-03T20:42:31.999Z');
        const expectedMin = ISODate('2023-03-03T20:42:27Z');
        const expectedMax = ISODate('2023-03-03T20:42:32Z');
        const actualMin = windowStats["minOpenWindowStartTime"];
        const actualMax = windowStats["maxOpenWindowStartTime"];
        const actualWatermark = stats["watermark"];
        try {
            assert.eq(expectedMin, actualMin);
            assert.eq(expectedMax, actualMax);
            assert.eq(expectedWatermark, actualWatermark);
            return true;
        } catch (e) {
            return false;
        }
    });

    insert([
        // belongs to 40-45 window through 44-49 window
        // sends 21:43.999 watermark, closing all open windows before this insert
        {timestamp: "2023-03-03T21:42:44.000Z", id: 11, value: 30},
    ]);
    assert.soon(() => {
        const stats = sp.window1.stats();
        const windowStats = stats.operatorStats[1];
        const expectedWatermark = ISODate('2023-03-03T21:42:43.999Z');
        const expectedMin = ISODate('2023-03-03T21:42:40Z');
        const expectedMax = ISODate('2023-03-03T21:42:44Z');
        const actualMin = windowStats["minOpenWindowStartTime"];
        const actualMax = windowStats["maxOpenWindowStartTime"];
        const actualWatermark = stats["watermark"];
        try {
            assert.eq(expectedMin, actualMin);
            assert.eq(expectedMax, actualMax);
            assert.eq(expectedWatermark, actualWatermark);
            return true;
        } catch (e) {
            return false;
        }
    });

    // Stop the streamProcessor.
    result = sp.window1.stop();
    assert.commandWorked(result);
}

runAll();
testWindowMinMaxStats();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);