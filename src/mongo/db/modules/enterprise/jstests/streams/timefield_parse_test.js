/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {uuidStr} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    listStreamProcessors,
    TEST_TENANT_ID
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

function testWithAtlasConn(testConfig, streams) {
    let dbName = "test";
    let dbConnectionName = "db1";
    let inputCollName = uuidStr();
    let outputCollName = uuidStr();
    let dlqCollName = uuidStr();
    let inputColl = db.getSiblingDB(dbName)[inputCollName];

    let pipeline = [
        {
            $source: {
                'connectionName': dbConnectionName,
                'db': dbName,
                'coll': inputCollName,
                'timeField': testConfig["timeField"]
            }
        },
        {$replaceRoot: {newRoot: '$fullDocument'}},
        {
            $tumblingWindow: {
                interval: {size: NumberInt(3), unit: "hour"},
                allowedLateness: NumberInt(0),
                pipeline: [{$group: {_id: "$customerId", customerDocs: {$push: "$$ROOT"}}}]
            }
        },
        {
            $merge: {
                into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName},
                whenMatched: 'replace',
                whenNotMatched: 'insert'
            }
        }
    ];

    let checkpointOptions = {storage: null, localDisk: {writeDirectory: "/tmp/checkpoints"}};

    let startOptions = {
        dlq: {connectionName: "db1", db: dbName, coll: dlqCollName},
        checkpointOptions: checkpointOptions,
        featureFlags: {},
    };

    let sp = streams.createStreamProcessor("timefield_parse_test-" + uuidStr(), pipeline);
    sp.start(startOptions);

    let inputDocs = [];
    for (let i = 1; i <= 10; i++) {
        inputDocs.push({customerId: i, ts: testConfig["ts"]});
    }
    inputColl.insert(inputDocs);

    assert.soon(() => { return sp.stats()["inputMessageCount"] == 10; });
    assert.soon(() => { return sp.stats()["outputMessageCount"] == 0; });

    var dlqCnt = 0;
    if (testConfig.expectedToFail) {
        dlqCnt = 10;
    }

    assert.soon(() => { return sp.stats()["dlqMessageCount"] == dlqCnt; });
    sp.stop();
}

function testTimeFieldsWithAtlasSource() {
    const uri = 'mongodb://' + db.getMongo().host;
    let connectionRegistry = [
        {name: "db1", type: 'atlas', options: {uri: uri}},
        {name: '__testMemory', type: 'in_memory', options: {}}
    ];

    let streams = new Streams(TEST_TENANT_ID, connectionRegistry);

    let testConfigs = [];
    let ts_str = "2023-01-01T00:00:00.000Z";
    let ts = ISODate(ts_str);
    // bad
    testConfigs.push({
        ts: ts_str,
        timeField: '$fullDocument.ts',
        expectedToFail: true,
    });
    // good
    testConfigs.push({
        ts: ts,
        timeField: '$fullDocument.ts',
        expectedToFail: false,
    });
    // good
    testConfigs.push({
        ts: ts_str,
        timeField: {$toDate: '$fullDocument.ts'},
        expectedToFail: false,
    });
    // good
    testConfigs.push({
        ts: ts,
        timeField: {$toDate: '$fullDocument.ts'},
        expectedToFail: false,
    });
    // bad
    testConfigs.push({
        ts: ts_str,
        timeField: {$toDate: {$multiply: ["$fullDocument.ts", 10]}},
        expectedToFail: true,
    });
    // bad
    testConfigs.push({
        ts: ts_str,
        timeField: "$$NOW",
        expectedToFail: true,
    });
    // bad
    testConfigs.push({
        ts: {$toDate: ts_str},
        timeField: "$fullDocument.ts",
        expectedToFail: true,
    });
    // bad
    testConfigs.push({
        ts: {$toDate: ts_str},
        timeField: {$toDate: "$fullDocument.ts"},
        expectedToFail: true,
    });
    // bad
    testConfigs.push({
        ts: ts_str,
        timeField: {$dateFromString: {"dateString": "$fullDocument.ts", format: "%d-%m-%Y"}},
        expectedToFail: true,
    });
    for (let testConfig of testConfigs) {
        jsTestLog("Testing with timeField=" + tojson(testConfig));
        testWithAtlasConn(testConfig, streams);
    }
}

testTimeFieldsWithAtlasSource();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);