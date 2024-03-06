/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {sanitizeDoc} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

(function() {
"use strict";

const inputColl = db.input_coll;
const foreignColl = db.foreign_coll;
const outputColl = db.output_coll;
const dlqColl = db.dlq_coll;

foreignColl.drop();
foreignColl.insert(Array.from({length: 20}, (_, i) => ({id: i, aa: i % 5, bb: Math.floor(i / 5)})));
assert.soon(() => { return foreignColl.count() == 20; });

function prepareDoc(doc) {
    doc = sanitizeDoc(sanitizeDoc(doc), /*fieldNames*/['_id']);
    for (var fieldName in doc) {
        if (Array.isArray(doc[fieldName])) {
            doc[fieldName] = doc[fieldName].map(
                (arrDoc) => { return sanitizeDoc(sanitizeDoc(arrDoc), /*fieldNames*/['_id']); });
        } else if (typeof doc[fieldName] === 'object') {
            doc[fieldName] = sanitizeDoc(sanitizeDoc(doc[fieldName]), /*fieldNames*/['_id']);
        }
    }
    return doc;
}

function startStreamProcessor(pipeline) {
    const uri = 'mongodb://' + db.getMongo().host;
    let startCmd = {
        streams_startStreamProcessor: '',
        tenantId: 'tenant1',
        name: 'lookupTest',
        processorId: 'lookupTest1',
        pipeline: pipeline,
        connections: [
            {name: "db1", type: 'atlas', options: {uri: uri}},
            {name: '__testMemory', type: 'in_memory', options: {}},
        ],
        options: {dlq: {connectionName: "db1", db: "test", coll: dlqColl.getName()}}
    };

    let result = db.runCommand(startCmd);
    jsTestLog(result);
    assert.eq(result["ok"], 1);
}

function stopStreamProcessor() {
    let stopCmd = {
        streams_stopStreamProcessor: '',
        name: 'lookupTest',
    };
    let result = db.runCommand(stopCmd);
    assert.eq(result["ok"], 1);
}

(function testBasic() {
    inputColl.drop();
    outputColl.drop();
    dlqColl.drop();

    // Start a stream processor.
    startStreamProcessor([
        {
            $source: {
                'connectionName': 'db1',
                'db': 'test',
                'coll': inputColl.getName(),
                'timeField': {$toDate: {$multiply: ['$fullDocument.ts', 1000]}},
                'allowedLateness': {'size': NumberInt(0), 'unit': 'second'}
            }
        },
        {$replaceRoot: {newRoot: '$fullDocument'}},
        {
            $lookup: {
                from: {connectionName: 'db1', db: 'test', coll: foreignColl.getName()},
                localField: "a",
                foreignField: "aa",
                as: 'arr',
            }
        },
        {
            $merge: {
                into: {connectionName: 'db1', db: 'test', coll: outputColl.getName()},
                whenMatched: 'replace',
                whenNotMatched: 'insert'
            }
        }
    ]);

    inputColl.insert([{id: 1, ts: 1, a: 1, b: 2, c: 3}, {id: 3, ts: 3, a: 3, b: 2, c: 3}]);

    assert.soon(() => { return outputColl.find().itcount() == 2; });
    assert.eq([{
                  id: 1,
                  ts: 1,
                  a: 1,
                  b: 2,
                  c: 3,
                  arr: [
                      {id: 1, aa: 1, bb: 0},
                      {id: 6, aa: 1, bb: 1},
                      {id: 11, aa: 1, bb: 2},
                      {id: 16, aa: 1, bb: 3}
                  ]
              }],
              outputColl.find({id: 1}).toArray().map(prepareDoc));
    assert.eq([{
                  id: 3,
                  ts: 3,
                  a: 3,
                  b: 2,
                  c: 3,
                  arr: [
                      {id: 3, aa: 3, bb: 0},
                      {id: 8, aa: 3, bb: 1},
                      {id: 13, aa: 3, bb: 2},
                      {id: 18, aa: 3, bb: 3}
                  ]
              }],
              outputColl.find({id: 3}).toArray().map(prepareDoc));

    // Stop the streamProcessor.
    stopStreamProcessor();
})();

(function testMultipleLookUps() {
    inputColl.drop();
    outputColl.drop();
    dlqColl.drop();

    // Start a stream processor.
    startStreamProcessor([
        {
            $source: {
                'connectionName': 'db1',
                'db': 'test',
                'coll': inputColl.getName(),
                'timeField': {$toDate: {$multiply: ['$fullDocument.ts', 1000]}},
                'allowedLateness': {'size': NumberInt(0), 'unit': 'second'}
            }
        },
        {$replaceRoot: {newRoot: '$fullDocument'}},
        {
            $lookup: {
                from: {connectionName: 'db1', db: 'test', coll: foreignColl.getName()},
                localField: "id",
                foreignField: "id",
                as: 'arr',
            }
        },
        {
            $lookup: {
                from: {connectionName: 'db1', db: 'test', coll: foreignColl.getName()},
                localField: "id",
                foreignField: "id",
                as: 'arr2',
            }
        },
        {
            $lookup: {
                from: {connectionName: 'db1', db: 'test', coll: foreignColl.getName()},
                localField: "id",
                foreignField: "id",
                as: 'arr3',
            }
        },
        {
            $merge: {
                into: {connectionName: 'db1', db: 'test', coll: outputColl.getName()},
                whenMatched: 'replace',
                whenNotMatched: 'insert'
            }
        }
    ]);

    inputColl.insert([{id: 1, ts: 1, a: 1, b: 2, c: 3}]);

    assert.soon(() => { return outputColl.find().itcount() == 1; });
    assert.eq([{
                  id: 1,
                  ts: 1,
                  a: 1,
                  b: 2,
                  c: 3,
                  arr: [
                      {id: 1, aa: 1, bb: 0},
                  ],
                  arr2: [
                      {id: 1, aa: 1, bb: 0},
                  ],
                  arr3: [
                      {id: 1, aa: 1, bb: 0},
                  ]
              }],
              outputColl.find({id: 1}).toArray().map(prepareDoc));

    // Stop the streamProcessor.
    stopStreamProcessor();
})();

(function testWithUnwind() {
    inputColl.drop();
    outputColl.drop();
    dlqColl.drop();

    // Start a stream processor.
    startStreamProcessor([
        {
            $source: {
                'connectionName': 'db1',
                'db': 'test',
                'coll': inputColl.getName(),
                'timeField': {$toDate: {$multiply: ['$fullDocument.ts', 1000]}},
                'allowedLateness': {'size': NumberInt(0), 'unit': 'second'}
            }
        },
        {$replaceRoot: {newRoot: '$fullDocument'}},
        {
            $lookup: {
                from: {connectionName: 'db1', db: 'test', coll: foreignColl.getName()},
                localField: "a",
                foreignField: "aa",
                as: 'arr',
            }
        },
        {
            $unwind: {
              path: "$arr"
            }
        },
        // This is needed to de-duplicate output documents.
        {
            $project: {
              _id: 0
            }
        },
        {
            $merge: {
                into: {connectionName: 'db1', db: 'test', coll: outputColl.getName()},
                whenMatched: 'replace',
                whenNotMatched: 'insert'
            }
        }
    ]);

    inputColl.insert([{id: 1, ts: 1, a: 1, b: 2, c: 3}, {id: 3, ts: 3, a: 3, b: 2, c: 3}]);

    assert.soon(() => { return outputColl.find().itcount() == 8; });
    assert.eq(
        [
            {id: 1, ts: 1, a: 1, b: 2, c: 3, arr: {id: 1, aa: 1, bb: 0}},
            {id: 1, ts: 1, a: 1, b: 2, c: 3, arr: {id: 6, aa: 1, bb: 1}},
            {id: 1, ts: 1, a: 1, b: 2, c: 3, arr: {id: 11, aa: 1, bb: 2}},
            {id: 1, ts: 1, a: 1, b: 2, c: 3, arr: {id: 16, aa: 1, bb: 3}}
        ],
        outputColl.find({id: 1}).sort({"arr.id": 1}).toArray().map(prepareDoc));
    assert.eq(
        [
            {id: 3, ts: 3, a: 3, b: 2, c: 3, arr: {id: 3, aa: 3, bb: 0}},
            {id: 3, ts: 3, a: 3, b: 2, c: 3, arr: {id: 8, aa: 3, bb: 1}},
            {id: 3, ts: 3, a: 3, b: 2, c: 3, arr: {id: 13, aa: 3, bb: 2}},
            {id: 3, ts: 3, a: 3, b: 2, c: 3, arr: {id: 18, aa: 3, bb: 3}}
        ],
        outputColl.find({id: 3}).sort({"arr.id": 1}).toArray().map(prepareDoc));

    // Stop the streamProcessor.
    stopStreamProcessor();
})();

(function testWithUnwindAndMatch() {
    inputColl.drop();
    outputColl.drop();
    dlqColl.drop();

    // Start a stream processor.
    startStreamProcessor([
        {
            $source: {
                'connectionName': 'db1',
                'db': 'test',
                'coll': inputColl.getName(),
                'timeField': {$toDate: {$multiply: ['$fullDocument.ts', 1000]}},
                'allowedLateness': {'size': NumberInt(0), 'unit': 'second'}
            }
        },
        {$replaceRoot: {newRoot: '$fullDocument'}},
        {
            $lookup: {
                from: {connectionName: 'db1', db: 'test', coll: foreignColl.getName()},
                localField: "a",
                foreignField: "aa",
                as: 'arr',
            }
        },
        {
            $unwind: {
              path: "$arr"
            }
        },
        {
            $match: {
              "arr.bb": { $lt: 2 }
            }
        },
        // This is needed to de-duplicate output documents.
        {
            $project: {
              _id: 0
            }
        },
        {
            $merge: {
                into: {connectionName: 'db1', db: 'test', coll: outputColl.getName()},
                whenMatched: 'replace',
                whenNotMatched: 'insert'
            }
        }
    ]);

    inputColl.insert([{id: 1, ts: 1, a: 1, b: 2, c: 3}, {id: 3, ts: 3, a: 3, b: 2, c: 3}]);

    assert.soon(() => { return outputColl.find().itcount() == 4; });
    assert.eq(
        [
            {id: 1, ts: 1, a: 1, b: 2, c: 3, arr: {id: 1, aa: 1, bb: 0}},
            {id: 1, ts: 1, a: 1, b: 2, c: 3, arr: {id: 6, aa: 1, bb: 1}},
        ],
        outputColl.find({id: 1}).sort({"arr.id": 1}).toArray().map(prepareDoc));
    assert.eq(
        [
            {id: 3, ts: 3, a: 3, b: 2, c: 3, arr: {id: 3, aa: 3, bb: 0}},
            {id: 3, ts: 3, a: 3, b: 2, c: 3, arr: {id: 8, aa: 3, bb: 1}},
        ],
        outputColl.find({id: 3}).sort({"arr.id": 1}).toArray().map(prepareDoc));

    // Stop the streamProcessor.
    stopStreamProcessor();
})();

(function testWithWindow() {
    inputColl.drop();
    outputColl.drop();
    dlqColl.drop();

    // Start a stream processor.
    startStreamProcessor([
        {
            $source: {
                'connectionName': 'db1',
                'db': 'test',
                'coll': inputColl.getName(),
                'timeField': {$toDate: '$fullDocument.ts'},
                'allowedLateness': {'size': NumberInt(0), 'unit': 'second'}
            }
        },
        {$replaceRoot: {newRoot: '$fullDocument'}},
        {
            $tumblingWindow: {
                interval: {size: NumberInt(1), unit: "second"},
                pipeline: [
                    {
                        $group: {
                            _id: "$id",
                            a: {$sum: "$a"},
                        }
                    },
                    {
                        $lookup: {
                            from: {connectionName: 'db1', db: 'test', coll: foreignColl.getName()},
                            localField: "a",
                            foreignField: "aa",
                            as: 'arr',
                        }
                    },
                ]
            }
        },
        {
            $merge: {
                into: {connectionName: 'db1', db: 'test', coll: outputColl.getName()},
                whenMatched: 'replace',
                whenNotMatched: 'insert'
            }
        }
    ]);

    // The last document is only needed for closing the windows we are interetesed in.
    inputColl.insertMany([
        {id: 1, ts: 1000, a: 1, b: 2, c: 3},
        {id: 3, ts: 3000, a: 3, b: 2, c: 3},
        {id: 5, ts: 5000}
    ]);

    assert.soon(() => { return outputColl.find().itcount() == 2; });
    assert.eq([{
                  a: 1,
                  arr: [
                      {id: 1, aa: 1, bb: 0},
                      {id: 6, aa: 1, bb: 1},
                      {id: 11, aa: 1, bb: 2},
                      {id: 16, aa: 1, bb: 3}
                  ]
              }],
              outputColl.find({_id: 1}).sort({"arr.id": 1}).toArray().map(prepareDoc));
    assert.eq([{
                  a: 3,
                  arr: [
                      {id: 3, aa: 3, bb: 0},
                      {id: 8, aa: 3, bb: 1},
                      {id: 13, aa: 3, bb: 2},
                      {id: 18, aa: 3, bb: 3}
                  ]
              }],
              outputColl.find({_id: 3}).sort({"arr.id": 1}).toArray().map(prepareDoc));

    // Stop the streamProcessor.
    stopStreamProcessor();
})();
}());
