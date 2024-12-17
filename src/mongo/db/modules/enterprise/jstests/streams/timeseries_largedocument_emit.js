/**
 * @tags: [
 *  featureFlagStreams,
 *  tsan_incompatible,
 * ]
 */

import {
    getOperatorStats,
    startStreamProcessor,
} from 'src/mongo/db/modules/enterprise/jstests/streams/timeseries_emit.js';
import {
    listStreamProcessors,
    stopStreamProcessor,
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

"use strict";

const inputColl = db.input_coll;
assert.commandWorked(
    db.createCollection("timeseries_coll", {timeseries: {timeField: 'ts', metaField: 'metaData'}}));
const timeseriesColl = db["timeseries_coll"];
const dlqColl = db.dlq_coll;

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
                allowedLateness: NumberInt(0),
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

    assert.soon(() => { return listStreamProcessors().streamProcessors.length == 1; });

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
    for (const dlqDoc of dlqColl.find().toArray()) {
        assert.eq(dlqDoc.operatorName, "TimeseriesEmitOperator");
    }
    assert.soon(() => { return timeseriesColl.count() == 3; });

    assert.soon(() => {
        let opStats = getOperatorStats("TimeseriesEmitOperator");
        return opStats["dlqMessageCount"] == 2;
    });

    assert.soon(() => {
        let opStats = getOperatorStats("TimeseriesEmitOperator");
        return (opStats["inputMessageCount"] == 5) && (opStats["outputMessageCount"] == 3);
    });

    stopStreamProcessor('timeseriesTest');
}

function testEmitToTimeSeriesLargeDocumentBatch() {
    jsTestLog("Running testEmitToTimeSeriesLargeDocumentBatch");

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

    const seed4 = Array(4 * 1024 * 1024 - 1000).toString();

    inputColl.insert(Array.from(
        {length: 5}, (_, i) => ({ts: ISODate("2024-03-01T01:00:01.000Z"), seed: seed4})));
    assert.eq(inputColl.count(), 5);
    assert.soon(() => { return timeseriesColl.count() == 5; });

    inputColl.insert(Array.from(
        {length: 5}, (_, i) => ({ts: ISODate("2024-03-01T01:00:02.000Z"), seed: seed4})));
    assert.eq(inputColl.count(), 10);
    assert.soon(() => { return timeseriesColl.count() == 10; });

    inputColl.insert(Array.from(
        {length: 5}, (_, i) => ({ts: ISODate("2024-03-01T01:00:03.000Z"), seed: seed4})));
    assert.eq(inputColl.count(), 15);
    assert.soon(() => { return timeseriesColl.count() == 15; });

    stopStreamProcessor('timeseriesTest');
}

testLargeDocumentEmitToTimeSeries();
testEmitToTimeSeriesLargeDocumentBatch();
inputColl.drop();
timeseriesColl.drop();
dlqColl.drop();
assert.eq(listStreamProcessors()["streamProcessors"].length, 0);