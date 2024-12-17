/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

"use strict";

import {
    startSample,
    sampleUntil,
    getStats,
    listStreamProcessors,
    stopStreamProcessor,
    sanitizeDoc,
    TEST_TENANT_ID
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const inputColl = db.input_coll;
const outColl = db.output_coll;
const dlqColl = db.dlq_coll;
const processorName = "testProcessorName";
const instanceName = "testInstanceName";

outColl.drop();
dlqColl.drop();
inputColl.drop();
assert.commandWorked(outColl.createIndex({c: 1}, {unique: true}));

function startStreamProcessor(pipeline) {
    const uri = 'mongodb://' + db.getMongo().host;
    let startCmd = {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: processorName,
        processorId: 'mergeTest1',
        pipeline: pipeline,
        connections: [{name: 'db1', type: 'atlas', options: {uri: uri}}],
        options: {
            dlq: {connectionName: "db1", db: "test", coll: dlqColl.getName()},
            featureFlags: {},
        },
        instanceName: instanceName
    };

    let result = db.runCommand(startCmd);
    jsTestLog(result);
    assert.eq(result["ok"], 1);
}

function getDlqOperatorStats() {
    let result = getStats(processorName);
    jsTestLog(result);

    let numDlqDocs = 0;
    let numDlqBytes = 0;
    let opStats = result["operatorStats"];
    jsTestLog(opStats);
    for (let i = 0; i < opStats.length; i++) {
        let op = opStats[i];
        jsTestLog(op);
        numDlqDocs += op["dlqMessageCount"];
        numDlqBytes += op["dlqMessageSize"];
    }

    return {numDlqDocs, numDlqBytes};
}

function getDlqStreamStats() {
    let result = getStats(processorName);
    jsTestLog(result);

    let numDlqDocs = result["dlqMessageCount"];
    let numDlqBytes = result["dlqMessageSize"];

    return {numDlqDocs, numDlqBytes};
}

const pipeline = [
    // Read from input_coll collection.
    {
        '$source': {
            'connectionName': 'db1',
            'db': 'test',
            'coll': 'input_coll',
            'timeField': {$toDate: {$multiply: ['$fullDocument.ts', 1000]}}
        }
    },
    {$replaceRoot: {newRoot: {$mergeObjects: ['$fullDocument', {_stream_meta: '$_stream_meta'}]}}},
    // Perform $a / $b. This runs into "can't $divide by zero" error when $b is 0.
    // This should add 10 documents to the dead letter queue.
    {$addFields: {division: {$divide: ['$a', '$b']}}},
    // Try to convert c to an int. This should add 1 document to the dead letter queue.
    {$addFields: {c_int: {$toInt: '$c'}}},
    {
        $tumblingWindow: {
            interval: {size: NumberInt(10), unit: 'second'},
            allowedLateness: NumberInt(0),
            pipeline: [
                // Perform $b / 0 when $b == 9. This runs into "can't $divide by zero"
                // error when $b == 9. This should add 10 documents to the dead letter
                // queue.
                {
                    $group: {
                        _id: null,
                        idMin: {$min: '$_id'},
                        c: {$first: '$c'},
                        bSum: {$sum: {$cond: [{$eq: ['$b', 9]}, {$divide: ['$b', 0]}, '$b']}}
                    }
                },
                {$addFields: {_id: '$idMin'}}
            ]
        }
    },
    // Perform $merge on 'c'.
    // This should add 5 documents to the dead letter queue since we are trying to
    // update _id field.
    {
        $merge: {
            into: {connectionName: 'db1', db: 'test', coll: 'output_coll'},
            whenMatched: 'replace',
            whenNotMatched: 'insert',
            on: 'c'
        }
    }
];

startStreamProcessor(pipeline);

assert.soon(() => { return listStreamProcessors().streamProcessors.length == 1; });

// Start a sample session.
const cursorId = startSample(processorName)["id"];

inputColl.insert(
    Array.from({length: 100}, (_, i) => ({_id: i, ts: i, a: i, b: i % 10, c: Math.floor(i / 20)})));
inputColl.insert({_id: 101, ts: 101, a: 101, b: 1, c: 'hello'});

assert.soon(() => { return dlqColl.count() == 26; });
assert.soon(() => { return outColl.count() == 5; });

// Verify the contents of a dlq doc.
const dlqDoc =
    dlqColl.find({"doc._id": 0}).toArray().map((doc) => sanitizeDoc(doc, ['_id', '_stream_meta']));
const dlqDocErrInfo = dlqDoc.map(doc => sanitizeDoc(doc.errInfo));
const dlqDocFullDoc = dlqDoc.map(doc => sanitizeDoc(doc.doc));
const expectedKeys = ["dlqTime", "processorName", "instanceName", "operatorName", "doc", "errInfo"];
assert.eq(Object.keys(dlqDoc[0]).length, expectedKeys.length);
assert.hasFields(
    dlqDoc[0],
    expectedKeys,
);
assert.eq(processorName, dlqDoc[0].processorName);
assert.eq(instanceName, dlqDoc[0].instanceName);
assert.eq(
    [{
        "reason":
            "Failed to process input document in AddFieldsOperator with error: can't $divide by zero"
    }],
    dlqDocErrInfo);
assert.eq([{"_id": 0, "ts": 0, "a": 0, "b": 0, "c": 0}], dlqDocFullDoc);
assert.eq(dlqDoc[0].operatorName, "AddFieldsOperator");

let sampledDocs = sampleUntil(cursorId, 5, processorName, 2);
let count = 0;
for (let i = 0; i < sampledDocs.length; i++) {
    if (!sampledDocs[i].hasOwnProperty("_dlqMessage")) {
        count++;
    }
}

assert.eq(count, 5);
assert.eq([{
              "_id": 1,
              "idMin": 1,
              "c": 0,
              "bSum": 36,
              "_stream_meta": {
                  "source": {
                      "type": "atlas",
                  },
                  "window": {
                      "start": ISODate("1970-01-01T00:00:00Z"),
                      "end": ISODate("1970-01-01T00:00:10Z"),
                  }
              }
          }],
          outColl.find({_id: 1}).toArray());
assert.eq([{
              "_id": 21,
              "idMin": 21,
              "c": 1,
              "bSum": 36,
              "_stream_meta": {
                  "source": {
                      "type": "atlas",
                  },
                  "window": {
                      "start": ISODate("1970-01-01T00:00:20Z"),
                      "end": ISODate("1970-01-01T00:00:30Z"),
                  }
              }
          }],
          outColl.find({_id: 21}).toArray());
assert.eq([{
              "_id": 41,
              "idMin": 41,
              "c": 2,
              "bSum": 36,
              "_stream_meta": {
                  "source": {
                      "type": "atlas",
                  },
                  "window": {
                      "start": ISODate("1970-01-01T00:00:40Z"),
                      "end": ISODate("1970-01-01T00:00:50Z"),
                  }
              }
          }],
          outColl.find({_id: 41}).toArray());
assert.eq([{
              "_id": 61,
              "idMin": 61,
              "c": 3,
              "bSum": 36,
              "_stream_meta": {
                  "source": {
                      "type": "atlas",
                  },
                  "window": {
                      "start": ISODate("1970-01-01T00:01:00Z"),
                      "end": ISODate("1970-01-01T00:01:10Z"),
                  }
              }
          }],
          outColl.find({_id: 61}).toArray());
assert.eq([{
              "_id": 81,
              "idMin": 81,
              "c": 4,
              "bSum": 36,
              "_stream_meta": {
                  "source": {
                      "type": "atlas",
                  },
                  "window": {
                      "start": ISODate("1970-01-01T00:01:20Z"),
                      "end": ISODate("1970-01-01T00:01:30Z"),
                  }
              }
          }],
          outColl.find({_id: 81}).toArray());

assert.soon(() => {
    let opStats = getDlqOperatorStats();
    let streamStats = getDlqStreamStats();
    return opStats.numDlqBytes > 0 && opStats.numDlqDocs == 26 && streamStats.numDlqDocs == 26 &&
        streamStats.numDlqBytes > 0 && streamStats.numDlqBytes == opStats.numDlqBytes;
});

const result = getStats(processorName);
// verify numOutputDocs matches the actual emitted docs;
jsTestLog(result);
assert.eq(result["ok"], 1);
assert.eq(result["outputMessageCount"], 5);
stopStreamProcessor(processorName);
outColl.drop();
dlqColl.drop();
inputColl.drop();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);