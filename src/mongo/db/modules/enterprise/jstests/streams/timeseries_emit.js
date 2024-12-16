/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {
    getStats,
    listStreamProcessors,
    stopStreamProcessor,
    TEST_TENANT_ID,
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

"use strict";

const inputColl = db.input_coll;
const outColl = db.output_coll;
assert.commandWorked(
    db.createCollection("timeseries_coll", {timeseries: {timeField: 'ts', metaField: 'metaData'}}));
const timeseriesColl = db["timeseries_coll"];
const dlqColl = db.dlq_coll;
const badUri = "mongodb://badUri";
const goodUri = 'mongodb://' + db.getMongo().host;
const spName = 'timeseriesTest';

export function startStreamProcessor(pipeline) {
    let startCmd = {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: 'timeseriesTest1',
        pipeline: pipeline,
        connections: [
            {name: 'db1', type: 'atlas', options: {uri: goodUri}},
            {name: 'db2', type: 'atlas', options: {uri: badUri}}
        ],
        options:
            {dlq: {connectionName: "db1", db: "test", coll: dlqColl.getName()}, featureFlags: {}}
    };

    let result = db.runCommand(startCmd);
    jsTestLog(result);
    return result;
}

export function getOperatorStats(operator = "") {
    let result = getStats(spName);
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

    assert.soon(() => { return listStreamProcessors().streamProcessors.length == 1; });

    inputColl.insert(
        Array.from({length: 10}, (_, i) => ({_id: i, ts: ISODate('2024-01-01T01:00:00Z')})));
    assert.soon(() => { return timeseriesColl.count() == 10; });
    inputColl.insert(Array.from({length: 10}, (_, i) => ({_id: i + 10, ts: i})));
    inputColl.insert(Array.from({length: 10}, (_, i) => ({_id: i + 20, ts: i})));
    assert.soon(() => { return dlqColl.count() == 20; });
    assert.eq(dlqColl.find({"operatorName": "TimeseriesEmitOperator"}).itcount(), 20);

    let opStats = getOperatorStats("TimeseriesEmitOperator");
    assert.soon(() => {
        opStats = getOperatorStats("TimeseriesEmitOperator");
        return opStats["dlqMessageCount"] == 20;
    });
    assert.eq(opStats["inputMessageCount"], 30);
    assert.eq(opStats["outputMessageCount"], 10);
    stopStreamProcessor(spName);
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

    assert.soon(() => { return listStreamProcessors().streamProcessors.length == 1; });

    inputColl.insert(
        Array.from({length: 10}, (_, i) => ({_id: i, ts: ISODate('2024-01-01T01:00:00Z')})));
    assert.soon(() => { return timeseriesColl.count() == 10; });
    inputColl.insert(Array.from({length: 10}, (_, i) => ({_id: i + 10, ts: i})));
    inputColl.insert(Array.from({length: 10}, (_, i) => ({_id: i + 20, ts: i})));
    assert.soon(() => { return dlqColl.count() == 20; });
    assert.eq(dlqColl.find({"operatorName": "TimeseriesEmitOperator"}).itcount(), 20);

    let opStats = getOperatorStats("TimeseriesEmitOperator");
    assert.soon(() => {
        opStats = getOperatorStats("TimeseriesEmitOperator");
        return opStats["dlqMessageCount"] == 20;
    });
    assert.eq(opStats["inputMessageCount"], 30);
    assert.eq(opStats["outputMessageCount"], 10);
    stopStreamProcessor(spName);
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

    assert.soon(() => { return listStreamProcessors().streamProcessors.length == 1; });

    inputColl.insert(
        Array.from({length: 100}, (_, i) => ({_id: i, ts: ISODate('2024-01-01T01:00:00Z')})));
    assert.soon(() => { return timeseriesColl.count() == 100; });
    inputColl.insert(Array.from({length: 50}, (_, i) => ({_id: i + 100, ts: i})));
    inputColl.insert(Array.from({length: 50}, (_, i) => ({_id: i + 150, ts: i})));

    assert.soon(() => { return dlqColl.count() == 100; });
    assert.eq(dlqColl.find({"operatorName": "TimeseriesEmitOperator"}).itcount(), 100);

    let opStats = getOperatorStats("TimeseriesEmitOperator");
    assert.soon(() => {
        opStats = getOperatorStats("TimeseriesEmitOperator");
        return opStats["dlqMessageCount"] == 100;
    });
    assert.eq(opStats["inputMessageCount"], 200);
    assert.eq(opStats["outputMessageCount"], 100);
    stopStreamProcessor(spName);
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

    assert.soon(() => { return listStreamProcessors().streamProcessors.length == 1; });

    inputColl.insert(
        Array.from({length: 100}, (_, i) => ({_id: i, ts: ISODate('2024-01-01T01:00:00Z')})));
    assert.soon(() => { return timeseriesColl.count() == 100; });
    inputColl.insert(Array.from({length: 100}, (_, i) => ({_id: i + 100, ts: i})));
    assert.soon(() => { return dlqColl.count() == 100; });
    assert.eq(dlqColl.find({"operatorName": "TimeseriesEmitOperator"}).itcount(), 100);

    let opStats = getOperatorStats("TimeseriesEmitOperator");
    assert.soon(() => {
        opStats = getOperatorStats("TimeseriesEmitOperator");
        return opStats["dlqMessageCount"] == 100;
    });
    assert.eq(opStats["inputMessageCount"], 200);
    assert.eq(opStats["outputMessageCount"], 100);
    stopStreamProcessor(spName);
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
    assert.includes(result["errmsg"], "$emit can only be used with a time series collection.");
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
    assert.includes(result["errmsg"], "$emit can only be used with a time series collection.");
}

function startStreamProcessorTimeseriesEmitDynamicContentRoutingFeatureFlagOn(pipeline) {
    let startCmd = {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: 'timeseriesTest1',
        pipeline: pipeline,
        connections: [
            {name: 'db1', type: 'atlas', options: {uri: goodUri}},
            {name: 'db2', type: 'atlas', options: {uri: badUri}}
        ],
        options: {
            dlq: {connectionName: "db1", db: "test", coll: dlqColl.getName()},
            featureFlags: {timeseriesEmitDynamicContentRouting: true}
        }
    };

    let result = db.runCommand(startCmd);
    jsTestLog(result);
    return result;
}

function testDynamicContentRoutingMultipleInputDocs() {
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
        // emit to the timeseries_coll collection
        {
            $emit: {
                connectionName: 'db1',
                db: 'test',
                coll: '$output_coll',
                timeseries: {timeField: 'ts', metaField: 'metaData'}
            }
        }
    ];
    let result = startStreamProcessorTimeseriesEmitDynamicContentRoutingFeatureFlagOn(pipeline);
    assert.eq(result["ok"], 1);
    assert.commandWorked(db.createCollection(
        "timeseries_coll2", {timeseries: {timeField: 'ts', metaField: 'metaData'}}));
    const timeseriesColl2 = db["timeseries_coll2"];

    inputColl.insertOne(
        {output_coll: timeseriesColl.getName(), ts: ISODate("2021-05-18T00:00:00.000Z")});
    inputColl.insertOne(
        {output_coll: timeseriesColl2.getName(), ts: ISODate("2021-05-18T00:00:00.000Z")});

    assert.soon(() => { return timeseriesColl.count() == 1; });
    assert.soon(() => { return timeseriesColl2.count() == 1; });

    stopStreamProcessor(spName);
}

function dynamicContentRoutingTestRunner({dbExpr, collExpr, inputDoc}) {
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
        // emit to the timeseries_coll collection
        {
            $emit: {
                connectionName: 'db1',
                db: dbExpr,
                coll: collExpr,
                timeseries: {timeField: 'ts', metaField: 'metaData'}
            }
        }
    ];

    let result = startStreamProcessorTimeseriesEmitDynamicContentRoutingFeatureFlagOn(pipeline);
    assert.eq(result["ok"], 1);
    assert.soon(() => { return listStreamProcessors().streamProcessors.length == 1; });

    inputColl.insertOne(inputDoc);
    assert.soon(() => { return timeseriesColl.count() == 1; });

    let opStats = getOperatorStats("TimeseriesEmitOperator");

    assert.eq(opStats["inputMessageCount"], 1);

    stopStreamProcessor(spName);
}

const testCases = [
    {
        dbExpr: '$bar',
        collExpr: {$concat: ['timeseries', '$foo']},
        inputDoc: {foo: "_coll", bar: "test", ts: ISODate("2021-05-18T00:00:00.000Z")}
    },
    {
        dbExpr: 'test',
        collExpr: {$concat: ['timeseries', '$foo']},
        inputDoc: {foo: "_coll", ts: ISODate("2021-05-18T00:00:00.000Z")}
    },
    {
        dbExpr: '$bar',
        collExpr: timeseriesColl.getName(),
        inputDoc: {bar: "test", ts: ISODate("2021-05-18T00:00:00.000Z")}
    },
];

for (const testCase of testCases) {
    jsTestLog(`Running test case ${tojson(testCase)}`);
    dynamicContentRoutingTestRunner(testCase);
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
testDynamicContentRoutingMultipleInputDocs();
inputColl.drop();
timeseriesColl.drop();
dlqColl.drop();
assert.eq(listStreamProcessors()["streamProcessors"].length, 0);