/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

"use strict";

const inputColl = db.input_coll;
const outColl = db.output_coll;
assert.commandWorked(
    db.createCollection("timeseries_coll", {timeseries: {timeField: 'ts', metaField: 'metaData'}}));
const timeseriesColl = db["timeseries_coll"];
const dlqColl = db.dlq_coll;
const badUri = "mongodb://badUri";
const goodUri = 'mongodb://' + db.getMongo().host;

function startStreamProcessor(pipeline) {
    let startCmd = {
        streams_startStreamProcessor: '',
        tenantId: 'tenant1',
        name: 'timeseriesTest',
        processorId: 'timeseriesTest1',
        pipeline: pipeline,
        connections: [
            {name: 'db1', type: 'atlas', options: {uri: goodUri}},
            {name: 'db2', type: 'atlas', options: {uri: badUri}}
        ],
        options: {dlq: {connectionName: "db1", db: "test", coll: dlqColl.getName()}}
    };

    let result = db.runCommand(startCmd);
    jsTestLog(result);
    return result;
}

function getOperatorStats(operator = "") {
    let getStatsCmd = {streams_getStats: '', name: 'timeseriesTest', verbose: true};
    let result = db.runCommand(getStatsCmd);
    if (result["ok"] != 1) {
        return 0;
    }

    let opStats = result["operatorStats"];
    jsTestLog(opStats);
    for (let i = 0; i < opStats.length; i++) {
        let op = opStats[i];
        if (op["name"] == operator) {
            return op;
        }
    }

    return opStats;
}

function stopStreamProcessor() {
    let stopCmd = {
        streams_stopStreamProcessor: '',
        name: 'timeseriesTest',
    };
    let result = db.runCommand(stopCmd);
    assert.eq(result["ok"], 1);
}

function testEmitToTimeSeriesCollection() {
    jsTestLog("Running testEmitToTimeSeriesCollection");
    const pipeline = [
        // Read from input_coll collection.
        {
            '$source': {
                'connectionName': 'db1',
                'db': 'test',
                'coll': 'input_coll',
                'timeField': {$toDate: '$ts'},
                'config': {'fullDocument': 'required', 'fullDocumentOnly': true}
            }
        },
        // emit to the timeseries_coll collection
        {
            $emit: {
                connectionName: 'db1',
                db: 'test',
                coll: timeseriesColl.getName(),
                timeseries: {timeField: 'ts', metaField: 'metaData'}
            }
        }
    ];

    let result = startStreamProcessor(pipeline);
    assert.eq(result["ok"], 1);

    let listCmd = {streams_listStreamProcessors: ''};
    assert.soon(() => { return db.runCommand(listCmd).streamProcessors.length == 1; });

    inputColl.insert(
        Array.from({length: 10}, (_, i) => ({_id: i, ts: ISODate('2024-01-01T01:00:00Z')})));
    assert.soon(() => { return timeseriesColl.count() == 10; });
    inputColl.insert(Array.from({length: 10}, (_, i) => ({_id: i + 10, ts: i})));
    inputColl.insert(Array.from({length: 10}, (_, i) => ({_id: i + 20, ts: i})));
    assert.soon(() => { return dlqColl.count() == 20; });

    let opStats = getOperatorStats("TimeseriesEmitOperator");
    assert.soon(() => {
        opStats = getOperatorStats("TimeseriesEmitOperator");
        return opStats["dlqMessageCount"] == 20;
    });
    assert.eq(opStats["inputMessageCount"], 30);
    assert.eq(opStats["outputMessageCount"], 10);
    stopStreamProcessor();
}

function testEmitToTimeSeriesMissingTimeField() {
    jsTestLog("Running testEmitToTimeSeriesMissingTimeField");

    inputColl.drop();
    timeseriesColl.drop();
    dlqColl.drop();
    assert.commandWorked(db.createCollection("timeseries_coll", {timeseries: {timeField: 'ts'}}));
    const pipeline = [
        // Read from input_coll collection.
        {
            '$source': {
                'connectionName': 'db1',
                'db': 'test',
                'coll': 'input_coll',
                'timeField': {$toDate: '$ts'},
                'config': {'fullDocument': 'required', 'fullDocumentOnly': true}
            }
        },
        // emit to the timeseries_coll collection
        {
            $emit: {
                connectionName: 'db1',
                db: 'test',
                coll: timeseriesColl.getName(),
                timeseries: {timeField: 'ts'}
            }
        }
    ];

    let result = startStreamProcessor(pipeline);
    assert.eq(result["ok"], 1);

    let listCmd = {streams_listStreamProcessors: ''};
    assert.soon(() => { return db.runCommand(listCmd).streamProcessors.length == 1; });

    inputColl.insert(
        Array.from({length: 10}, (_, i) => ({_id: i, ts: ISODate('2024-01-01T01:00:00Z')})));
    assert.soon(() => { return timeseriesColl.count() == 10; });
    inputColl.insert(Array.from({length: 10}, (_, i) => ({_id: i + 10, ts: i})));
    inputColl.insert(Array.from({length: 10}, (_, i) => ({_id: i + 20, ts: i})));
    assert.soon(() => { return dlqColl.count() == 20; });

    let opStats = getOperatorStats("TimeseriesEmitOperator");
    assert.soon(() => {
        opStats = getOperatorStats("TimeseriesEmitOperator");
        return opStats["dlqMessageCount"] == 20;
    });
    assert.eq(opStats["inputMessageCount"], 30);
    assert.eq(opStats["outputMessageCount"], 10);
    stopStreamProcessor();
}

// test missing timeseries field in $emit
function testMissingTimeseries() {
    jsTestLog("Running testMissingTimeseries");

    inputColl.drop();
    timeseriesColl.drop();
    dlqColl.drop();
    assert.commandWorked(db.createCollection("timeseries_coll", {timeseries: {timeField: 'ts'}}));
    const pipeline = [
        // Read from input_coll collection.
        {
            '$source': {
                'connectionName': 'db1',
                'db': 'test',
                'coll': 'input_coll',
                'timeField': {$toDate: '$ts'},
                'config': {'fullDocument': 'required', 'fullDocumentOnly': true}
            }
        },
        // emit to the timeseries_coll collection, with missing timeseries field
        {$emit: {connectionName: 'db1', db: 'test', coll: timeseriesColl.getName()}}
    ];

    let result = startStreamProcessor(pipeline);
    assert.eq(result["ok"], 1);

    let listCmd = {streams_listStreamProcessors: ''};
    assert.soon(() => { return db.runCommand(listCmd).streamProcessors.length == 1; });

    inputColl.insert(
        Array.from({length: 100}, (_, i) => ({_id: i, ts: ISODate('2024-01-01T01:00:00Z')})));
    assert.soon(() => { return timeseriesColl.count() == 100; });
    inputColl.insert(Array.from({length: 50}, (_, i) => ({_id: i + 100, ts: i})));
    inputColl.insert(Array.from({length: 50}, (_, i) => ({_id: i + 150, ts: i})));

    assert.soon(() => { return dlqColl.count() == 100; });

    let opStats = getOperatorStats("TimeseriesEmitOperator");
    assert.soon(() => {
        opStats = getOperatorStats("TimeseriesEmitOperator");
        return opStats["dlqMessageCount"] == 100;
    });
    assert.eq(opStats["inputMessageCount"], 200);
    assert.eq(opStats["outputMessageCount"], 100);
    stopStreamProcessor();
}

function testMissingTimeseriesCollection() {
    jsTestLog("Running testMissingTimeseries");

    inputColl.drop();
    timeseriesColl.drop();
    dlqColl.drop();
    // assert.commandWorked(db.createCollection("timeseries_coll", {timeseries: {timeField:
    // 'ts'}}));
    const pipeline = [
        // Read from input_coll collection.
        {
            '$source': {
                'connectionName': 'db1',
                'db': 'test',
                'coll': 'input_coll',
                'timeField': {$toDate: '$ts'},
                'config': {'fullDocument': 'required', 'fullDocumentOnly': true}
            }
        },
        // emit to the timeseries_coll collection, with missing timeseries field
        {
            $emit: {
                connectionName: 'db1',
                db: 'test',
                coll: timeseriesColl.getName(),
                timeseries: {timeField: 'ts', metaField: 'metaData'}
            }
        }
    ];

    let result = startStreamProcessor(pipeline);
    assert.eq(result["ok"], 1);

    let listCmd = {streams_listStreamProcessors: ''};
    assert.soon(() => { return db.runCommand(listCmd).streamProcessors.length == 1; });

    inputColl.insert(
        Array.from({length: 100}, (_, i) => ({_id: i, ts: ISODate('2024-01-01T01:00:00Z')})));
    assert.soon(() => { return timeseriesColl.count() == 100; });
    inputColl.insert(Array.from({length: 100}, (_, i) => ({_id: i + 100, ts: i})));
    assert.soon(() => { return dlqColl.count() == 100; });

    let opStats = getOperatorStats("TimeseriesEmitOperator");
    assert.soon(() => {
        opStats = getOperatorStats("TimeseriesEmitOperator");
        return opStats["dlqMessageCount"] == 100;
    });
    assert.eq(opStats["inputMessageCount"], 200);
    assert.eq(opStats["outputMessageCount"], 100);
    stopStreamProcessor();
}

function testBadUri() {
    jsTestLog("Running testBadUri");

    inputColl.drop();
    timeseriesColl.drop();
    dlqColl.drop();
    assert.commandWorked(db.createCollection("timeseries_coll", {timeseries: {timeField: 'ts'}}));
    const pipeline = [
        // Read from input_coll collection.
        {
            '$source': {
                'connectionName': 'db1',
                'db': 'test',
                'coll': 'input_coll',
                'timeField': {$toDate: '$ts'},
                'config': {'fullDocument': 'required', 'fullDocumentOnly': true}
            }
        },
        // emit to the timeseries_coll collection, with missing timeseries field
        {
            $emit: {
                connectionName: 'db2',
                db: 'test',
                coll: timeseriesColl.getName(),
                timeseries: {timeField: 'ts', metaField: 'metaData'}
            }
        }
    ];

    let result = startStreamProcessor(pipeline);
    assert.eq(result["ok"], 0);
}

function testIncorrectCollectionType() {
    jsTestLog("Running testIncorrectCollectionType");

    inputColl.drop();
    timeseriesColl.drop();
    dlqColl.drop();
    assert.commandWorked(db.createCollection("timeseries_coll"));
    const pipeline = [
        // Read from input_coll collection.
        {
            '$source': {
                'connectionName': 'db1',
                'db': 'test',
                'coll': 'input_coll',
                'timeField': {$toDate: '$ts'},
                'config': {'fullDocument': 'required', 'fullDocumentOnly': true}
            }
        },
        // emit to the timeseries_coll collection, with missing timeseries field
        {
            $emit: {
                connectionName: 'db1',
                db: 'test',
                coll: timeseriesColl.getName(),
                timeseries: {timeField: 'ts', metaField: 'metaData'}
            }
        }
    ];

    let result = startStreamProcessor(pipeline);
    assert.eq(result["ok"], 0);
    assert.includes(result["errmsg"], "Expected a Time Series collection");
}

function testTimeFieldMismatch() {
    jsTestLog("Running testTimeFieldMismatch");

    inputColl.drop();
    timeseriesColl.drop();
    dlqColl.drop();
    assert.commandWorked(db.createCollection("timeseries_coll", {timeseries: {timeField: 'ts'}}));
    const pipeline = [
        // Read from input_coll collection.
        {
            '$source': {
                'connectionName': 'db1',
                'db': 'test',
                'coll': 'input_coll',
                'timeField': {$toDate: '$ts'},
                'config': {'fullDocument': 'required', 'fullDocumentOnly': true}
            }
        },
        // emit to the timeseries_coll collection, with missing timeseries field
        {
            $emit: {
                connectionName: 'db1',
                db: 'test',
                coll: timeseriesColl.getName(),
                timeseries: {timeField: '_ts', metaField: 'metaData'}
            }
        }
    ];

    let result = startStreamProcessor(pipeline);
    assert.eq(result["ok"], 0);
    assert.includes(result["errmsg"], "timeField ts that doesn't match the $emit.timeField _ts");
}

function testMissingTimeField() {
    jsTestLog("Running testMissingTimeField");

    inputColl.drop();
    timeseriesColl.drop();
    dlqColl.drop();
    // assert.commandWorked(db.createCollection("timeseries_coll", {timeseries: {timeField:
    // 'ts'}}));
    const pipeline = [
        // Read from input_coll collection.
        {
            '$source': {
                'connectionName': 'db1',
                'db': 'test',
                'coll': 'input_coll',
                'timeField': {$toDate: '$ts'},
                'config': {'fullDocument': 'required', 'fullDocumentOnly': true}
            }
        },
        // emit to the timeseries_coll collection, with missing timeseries field
        {
            $emit: {
                connectionName: 'db1',
                db: 'test',
                coll: timeseriesColl.getName(),
                timeseries: {metaField: 'metaData'}
            }
        }
    ];

    let result = startStreamProcessor(pipeline);
    assert.eq(result["ok"], 0);
    assert.includes(result["errmsg"],
                    "BSON field 'TimeseriesSinkOptions.timeseries.timeField' is missing");
}

function testNoTimeFieldNoCollection() {
    jsTestLog("Running testNoTimeFieldNoCollection");

    inputColl.drop();
    timeseriesColl.drop();
    dlqColl.drop();
    const pipeline = [
        // Read from input_coll collection.
        {
            '$source': {
                'connectionName': 'db1',
                'db': 'test',
                'coll': 'input_coll',
                'timeField': {$toDate: '$ts'},
                'config': {'fullDocument': 'required', 'fullDocumentOnly': true}
            }
        },
        // emit to the timeseries_coll collection, with missing timeseries field
        {$emit: {connectionName: 'db1', db: 'test', coll: timeseriesColl.getName()}}
    ];

    let result = startStreamProcessor(pipeline);
    assert.eq(result["ok"], 0);
    assert.includes(result["errmsg"], "$emit.timeSeries must be specified");
}

function testNoTimeFieldNoTimeSeriesCollection() {
    jsTestLog("Running testNoTimeFieldNoTimeSeriesCollection");

    inputColl.drop();
    timeseriesColl.drop();
    dlqColl.drop();
    assert.commandWorked(db.createCollection("timeseries_coll"));
    const pipeline = [
        // Read from input_coll collection.
        {
            '$source': {
                'connectionName': 'db1',
                'db': 'test',
                'coll': 'input_coll',
                'timeField': {$toDate: '$ts'},
                'config': {'fullDocument': 'required', 'fullDocumentOnly': true}
            }
        },
        // emit to the timeseries_coll collection, with missing timeseries field
        {$emit: {connectionName: 'db1', db: 'test', coll: timeseriesColl.getName()}}
    ];

    let result = startStreamProcessor(pipeline);
    assert.eq(result["ok"], 0);
    assert.includes(result["errmsg"], "Expected a Time Series collection");
}

// For a Time Series collection, the max document size is limited to 4MB.
function testLargeDocumentEmitToTimeSeries() {
    jsTestLog("Running testLargeDocumentEmitToTimeSeries");

    inputColl.drop();
    timeseriesColl.drop();
    dlqColl.drop();

    const pipeline = [
        // Read from input_coll collection.
        {
            $source: {
                connectionName: 'db1',
                db: 'test',
                coll: 'input_coll',
                timeField: '$ts',
                config: {fullDocument: 'required', fullDocumentOnly: true}
            }
        },
        {
            $tumblingWindow: {
                interval: {size: NumberInt(10), unit: 'second'},
                allowedLateness: {size: NumberInt(0), unit: 'second'},
                pipeline: [
                    { $project: { docSize: 1, seed: 1, ts: 1, value: { $range: [ 0, "$docCount" ] } } },
                    { $unwind: "$value" },
                    { $project: { seed: 1, ts: 1, bigValue: { $range: [0, "$docSize"] }}},
                    { $project: { bigStr: { $reduce: { input: "$bigValue", initialValue: ".", in: {"$concat": [ "$$value", "$seed" ]}}}, ts: 1}},
                    { $group: { _id: "$_id", bigArr: {$push: "$bigStr"}, ts: {$max: "$ts"}}}
                ]
            }
        },
        // emit to the timeseries_coll collection
        {
            $emit: {
                connectionName: 'db1',
                db: 'test',
                coll: timeseriesColl.getName(),
                timeseries: {timeField: 'ts'}
            }
        }
    ];

    let result = startStreamProcessor(pipeline);
    assert.eq(result["ok"], 1);

    let listCmd = {streams_listStreamProcessors: ''};
    assert.soon(() => { return db.runCommand(listCmd).streamProcessors.length == 1; });

    // Seed of size 8KB.
    const seed = Array(8 * 1024).toString();
    // Accumulate size of ~1MB.
    inputColl.insert(
        {_id: 1, ts: ISODate("2024-03-01T01:00:00.000Z"), docCount: 8, docSize: 16, seed: seed});
    // Accumulate size of ~500KB.
    inputColl.insert(
        {_id: 2, ts: ISODate("2024-03-01T01:00:00.000Z"), docCount: 8, docSize: 8, seed: seed});
    // Accumulate size of >4MB, this should result in dlq.
    inputColl.insert(
        {_id: 3, ts: ISODate("2024-03-01T01:00:00.000Z"), docCount: 8, docSize: 64, seed: seed});
    // Accumulate size of ~3.75MB.
    inputColl.insert(
        {_id: 4, ts: ISODate("2024-03-01T01:00:00.000Z"), docCount: 8, docSize: 60, seed: seed});
    // Accumulate size of >4MB, this should result in dlq.
    inputColl.insert(
        {_id: 5, ts: ISODate("2024-03-01T01:00:00.000Z"), docCount: 16, docSize: 60, seed: seed});
    // Close the window.
    inputColl.insert(
        {_id: 6, ts: ISODate("2024-03-01T05:00:00.000Z"), docCount: 1, docSize: 1, seed: seed});

    assert.soon(() => { return dlqColl.count() == 2; });
    assert.soon(() => { return timeseriesColl.count() == 3; });

    let opStats = getOperatorStats("TimeseriesEmitOperator");
    assert.soon(() => {
        opStats = getOperatorStats("TimeseriesEmitOperator");
        return opStats["dlqMessageCount"] == 2;
    });
    assert.eq(opStats["inputMessageCount"], 5);
    assert.eq(opStats["outputMessageCount"], 3);

    stopStreamProcessor();
}

testEmitToTimeSeriesCollection();
testEmitToTimeSeriesMissingTimeField();
testMissingTimeseries();
testMissingTimeseriesCollection();
testBadUri();
testIncorrectCollectionType();
testTimeFieldMismatch();
testMissingTimeField();
testNoTimeFieldNoCollection();
testNoTimeFieldNoTimeSeriesCollection();
testLargeDocumentEmitToTimeSeries();
inputColl.drop();
timeseriesColl.drop();
dlqColl.drop();