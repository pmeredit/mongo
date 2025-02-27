/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {getDefaultSp, test} from 'src/mongo/db/modules/enterprise/jstests/streams/fake_client.js';
import {
    listStreamProcessors,
    verifyInputEqualsOutput
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

function smokeTest() {
    const sp = getDefaultSp();
    const spName = "groupDlq";
    const inputColl = db.getSiblingDB(test.dbName)[test.inputCollName];
    const dlqColl = db.getSiblingDB(test.dbName)["dlqColl"];
    const outputColl = db.getSiblingDB(test.dbName)[test.outputCollName];
    inputColl.drop();
    outputColl.drop();
    dlqColl.drop();
    sp.createStreamProcessor(spName, [
        {
            $source: {
                connectionName: test.atlasConnection,
                db: test.dbName,
                coll: test.inputCollName,
                timeField: {$toDate: "$fullDocument.ts"},
                config: {pipeline: [{$match: {operationType: "insert"}}]}
            }
        },
        {
            $tumblingWindow: {
                pipeline: [{
                    $group:
                        {_id: {$divide: ["$fullDocument.y", "$fullDocument.x"]}, count: {$sum: 1}}
                }],
                interval: {size: NumberInt(1), unit: 'second'},
                allowedLateness: {size: NumberInt(0), unit: 'second'}
            }
        },
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
    sp[spName].start({
        dlq: {connectionName: test.atlasConnection, db: test.dbName, coll: "dlqColl"},
        featureFlags: {}
    });
    inputColl.insertMany([
        {x: 2, y: 30, ts: "2023-03-03T20:42:30.000Z"},
        // This will be DLQ-ed.
        {x: 0, y: 10, ts: "2023-03-03T20:42:30.000Z"},
        {x: 1, y: 25, ts: "2023-03-03T20:42:30.000Z"},
        {x: 1, y: 25, ts: "2023-03-03T20:42:30.000Z"},
        // This will close the first window.
        {x: 1, y: 25, ts: "2023-03-03T20:42:32.000Z"},
    ]);
    assert.soon(() => { return outputColl.count() == 2 && dlqColl.count() == 1; });
    assert.eq(
        [
            {
                _id: 15,
                _stream_meta: {
                    "source": {
                        "type": "atlas",
                    },
                    "window": {
                        "start": ISODate("2023-03-03T20:42:30Z"),
                        "end": ISODate("2023-03-03T20:42:31Z"),
                    }
                },
                count: 1
            },
            {
                _id: 25,
                _stream_meta: {
                    "source": {
                        "type": "atlas",
                    },
                    "window": {
                        "start": ISODate("2023-03-03T20:42:30Z"),
                        "end": ISODate("2023-03-03T20:42:31Z"),
                    }
                },
                count: 2
            },
        ],
        outputColl.find().sort({_id: 1}).toArray());

    var dlqDocs = dlqColl.find().sort({_id: 1}).toArray();
    verifyInputEqualsOutput(
        [{
            "reason":
                "Failed to process input document in GroupOperator with error: can't $divide by zero"
        }],
        dlqDocs.map(doc => doc.errInfo));

    for (const dlqDoc of dlqDocs) {
        assert.eq(dlqDoc.operatorName, "GroupOperator");
    }

    // Stop the streamProcessor.
    sp[spName].stop();
}

smokeTest();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);