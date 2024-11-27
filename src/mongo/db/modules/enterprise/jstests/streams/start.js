/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {Thread} from "jstests/libs/parallelTester.js";
import {getDefaultSp, test} from 'src/mongo/db/modules/enterprise/jstests/streams/fake_client.js';
import {
    listStreamProcessors,
    stopStreamProcessor,
    TEST_TENANT_ID
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

(function() {
"use strict";

const uri = 'mongodb://' + db.getMongo().host;
const dbConnectionName = "db1";
const dbName = "test";
const inputCollName = "input_coll";
const inputColl = db.getSiblingDB(dbName)[inputCollName];
const dlqCollName = "dlq_coll";
const dlqColl = db.getSiblingDB(dbName)[dlqCollName];
const outputCollName = "output_coll";
const outputColl = db.getSiblingDB(dbName)[outputCollName];
const spName = "sp1";
const connectionRegistry = [{name: dbConnectionName, type: 'atlas', options: {uri: uri}}];

function startStreamProcessor(pipeline,
                              startOptions = {
                                  featureFlags: {}
                              },
                              validateSuccess = true) {
    let startCmd = {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: spName,
        processorId: 'spName1',
        pipeline: pipeline,
        connections: [{name: dbConnectionName, type: 'atlas', options: {uri: uri}}],
        options: startOptions
    };

    let result = db.runCommand(startCmd);
    jsTestLog(result);
    if (validateSuccess) {
        assert.commandWorked(result);
    }
    return result;
}

(function startNormal() {
    inputColl.drop();
    dlqColl.drop();
    outputColl.drop();

    // Calls streams_startStreamProcessor with validateOnly: true.
    let result = startStreamProcessor([
        {$source: {connectionName: dbConnectionName, db: dbName, coll: inputCollName}},
        {$merge: {into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName}}}
    ]);
    assert.eq(result["optimizedPipeline"], [
        {$source: {connectionName: dbConnectionName, db: dbName, coll: inputCollName}},
        {$merge: {into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName}}}
    ]);

    // Insert some documents into the "input column".
    inputColl.insert([
        {id: 0, value: 1},
        {id: 1, value: 1},
        {id: 2, value: 1},
        {id: 3, value: 1},
    ]);

    assert.soon(() => {
        let result = db.runCommand(
            {streams_getStats: '', tenantId: TEST_TENANT_ID, name: spName, verbose: true});
        assert.commandWorked(result);
        assert.eq(result["ok"], 1);
        const operatorStats = result["operatorStats"];
        if (operatorStats.length > 0) {
            assert.eq("ChangeStreamConsumerOperator", operatorStats[0]["name"]);
            return operatorStats[0].inputMessageCount >= 4;
        }
        return false;
    });

    stopStreamProcessor(spName);
}());

(function startEnableDataFlow() {
    inputColl.drop();
    dlqColl.drop();
    outputColl.drop();

    // Calls streams_startStreamProcessor with enableDataFlow: false.
    startStreamProcessor(
        [
            {$source: {connectionName: dbConnectionName, db: dbName, coll: inputCollName}},
            {$merge: {into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName}}}
        ],
        {enableDataFlow: false, featureFlags: {}});

    // Insert some documents into the "input column".
    inputColl.insert([
        {id: 0, value: 1},
        {id: 1, value: 1},
        {id: 2, value: 1},
        {id: 3, value: 1},
    ]);
    // Wait 3 seconds and verify nothing is in the output even though we sent some input.
    sleep(3000);

    // Wait until all 4 input docs are read by the source operator.
    assert.soon(() => {
        let getMetricsCmd = {streams_getMetrics: ''};
        let result = db.runCommand(getMetricsCmd);
        assert.eq(result["ok"], 1);
        let metric =
            result["gauges"].filter(metric => metric.name === "source_operator_queue_size");
        assert.eq(metric.length, 1);
        return metric[0].value >= 4;
    });
    assert.eq(0, outputColl.find({}).toArray().length);

    stopStreamProcessor(spName);
}());

(function startValidate() {
    inputColl.drop();
    dlqColl.drop();
    outputColl.drop();

    // Calls streams_startStreamProcessor with validateOnly: true.
    startStreamProcessor(
        [
            {$source: {connectionName: dbConnectionName, db: dbName, coll: inputCollName}},
            {$merge: {into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName}}}
        ],
        {validateOnly: true, featureFlags: {}});

    // Insert some documents into the "input column".
    inputColl.insert([
        {id: 0, value: 1},
        {id: 1, value: 1},
        {id: 2, value: 1},
        {id: 3, value: 1},
    ]);
    // Validate the streamProcessor was not actually started.
    // Validate nothing shows up in listStreamProcessors.
    let result = db.runCommand({streams_listStreamProcessors: '', tenantId: TEST_TENANT_ID});
    assert.commandWorked(result);
    assert.eq(result["streamProcessors"].length, 0, result);
    // Wait 3 seconds and verify nothing is in the output even though we sent some input.
    sleep(3000);
    assert.eq(0, outputColl.find({}).toArray().length);

    // Calls streams_startStreamProcessor with { validateOnly: true } with a few
    // invalid requests. Verify the command fails.

    result = startStreamProcessor(
        [
            {
                $source: {
                    connectionName: dbConnectionName,
                    db: dbName,
                    coll: inputCollName,
                    // Invalid field name.
                    foo: 1
                }
            },
            {$merge: {into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName}}}
        ],
        {validateOnly: true, featureFlags: {}},
        false /* validateSuccess */);
    assert.commandFailed(result);

    result = startStreamProcessor(
        [
            {
                $source: {
                    connectionName: dbConnectionName,
                    db: dbName,
                    coll: inputCollName,
                }
            },
            // $group not supported at the top level, only inside windows.
            {$group: {_id: null, out: {$count: {}}}},
            {$merge: {into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName}}}
        ],
        {validateOnly: true},
        false /* validateSuccess */);
    assert.commandFailed(result);

    result = startStreamProcessor(
        [
            {
                $source: {
                    connectionName: "thisDbDoesNotExist",
                    db: dbName,
                    coll: inputCollName,
                }
            },
            {$merge: {into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName}}}
        ],
        {validateOnly: true, featureFlags: {}},
        false /* validateSuccess */);
    assert.commandFailed(result);

    result = startStreamProcessor(
        [
            {
                $source: {
                    connectionName: dbConnectionName,
                    db: dbName,
                    coll: inputCollName,
                }
            },
            {$merge: {into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName}}}
        ],
        {
            validateOnly: true,
            dlq: {
                connectionName: "thisDlqDoesntExist",
                db: dbName,
                coll: dlqCollName,
            },
            featureFlags: {},
        },
        false /* validateSuccess */);
    assert.commandFailed(result);
}());

// This is a regression test for an issue we found in prod.
// The test issues a stop request shortly after a start request.
// Prior to the fix this could sometimes cause an invariant.
(function stopDuringStart() {
    // Turn on a failpoint that will make changestream $source sleep for a bit in its background
    // connection routine. This makes the problem easier to hit.
    assert.commandWorked(db.adminCommand(
        {'configureFailPoint': 'changestreamSourceSleepBeforeConnect', 'mode': 'alwaysOn'}));

    const sp = getDefaultSp();
    const spName = "stopDuringStart";
    sp.createStreamProcessor(spName, [
        {
            $source:
                {connectionName: test.atlasConnection, db: test.dbName, coll: test.inputCollName}
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

    let stopThread = new Thread((spName, tenantId) => {
        sleep(300);
        db.runCommand({streams_stopStreamProcessor: "", tenantId: tenantId, name: spName});
    }, spName, TEST_TENANT_ID);
    stopThread.start();
    sp[spName].start({featureFlags: {}}, /* assertWorked */ false);
    stopThread.join();

    assert.commandWorked(db.adminCommand(
        {'configureFailPoint': 'changestreamSourceSleepBeforeConnect', 'mode': 'off'}));
}());

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
}());
