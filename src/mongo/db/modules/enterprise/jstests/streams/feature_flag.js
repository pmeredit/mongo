import {
    listStreamProcessors,
    startStreamProcessor,
    stopStreamProcessor,
    TEST_PROJECT_ID,
    TEST_TENANT_ID
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const spName = "featureFlagTest";
const inputCollName = "testin";
const outputCollName = 'outputColl';
const outputColl = db[outputCollName];
const inputColl = db[inputCollName];

startStreamProcessor(spName, [
    {
        $source: {
            'connectionName': 'db1',
            'db': 'test',
            'coll': inputColl.getName(),
            'timeField': {$toDate: {$multiply: ['$fullDocument.ts', 1000]}}
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
let result =
    db.runCommand({streams_testOnlyGetFeatureFlags: '', tenantId: TEST_TENANT_ID, name: spName});
jsTestLog(result);
assert.eq(result.featureFlags, {});

let ff = {feature_flag_a: {value: true}};
ff.feature_flag_a["streamProcessors"] = {};
ff.feature_flag_a.streamProcessors[spName] = false;

result =
    db.runCommand({streams_updateFeatureFlags: '', tenantId: TEST_TENANT_ID, featureFlags: ff});
assert.eq(result.ok, true);

assert.soon(() => {
    result = db.runCommand(
        {streams_testOnlyGetFeatureFlags: '', tenantId: TEST_TENANT_ID, name: spName});
    return (result.featureFlags.hasOwnProperty("feature_flag_a") &&
            !result.featureFlags.feature_flag_a);
});

ff = {
    feature_flag_a: {value: true},
    feature_flag_b: {streamProcessors: {sp1: true}}
};
ff.feature_flag_a["streamProcessors"] = {};
ff.feature_flag_a.streamProcessors["someOtherSp"] = false;

result =
    db.runCommand({streams_updateFeatureFlags: '', tenantId: TEST_TENANT_ID, featureFlags: ff});
assert.eq(result.ok, true);

assert.soon(() => {
    result = db.runCommand(
        {streams_testOnlyGetFeatureFlags: '', tenantId: TEST_TENANT_ID, name: spName});
    return (result.featureFlags.feature_flag_a && !result.featureFlags.feature_flag_b);
});

stopStreamProcessor(spName);

const pipeline = [
    {
        $source: {
            'connectionName': 'db1',
            'db': 'test',
            'coll': inputColl.getName(),
            'timeField': {$toDate: {$multiply: ['$fullDocument.ts', 1000]}}
        }
    },
    {
        $merge: {
            into: {connectionName: 'db1', db: 'test', coll: outputColl.getName()},
            whenMatched: 'replace',
            whenNotMatched: 'insert'
        }
    }
];

const uri = 'mongodb://' + db.getMongo().host;
let startCmd = {
    streams_startStreamProcessor: '',
    tenantId: TEST_TENANT_ID,
    projectId: TEST_PROJECT_ID,
    name: spName,
    processorId: spName,
    pipeline: pipeline,
    connections: [
        {name: "db1", type: 'atlas', options: {uri: uri}},
        {name: '__testMemory', type: 'in_memory', options: {}},
        {
            name: "kafka1",
            type: 'kafka',
            options: {bootstrapServers: 'localhost:9092', isTestKafka: true},
        },
    ],
    options: {
        dlq: {connectionName: "db1", db: jsTestName(), coll: "dlq"},
        featureFlags: {feature_flag_a: true, feature_flag_b: false}
    }
};
assert.commandWorked(db.runCommand(startCmd));

assert.soon(() => {
    result = db.runCommand(
        {streams_testOnlyGetFeatureFlags: '', tenantId: TEST_TENANT_ID, name: spName});
    return (result.featureFlags.feature_flag_a && !result.featureFlags.feature_flag_b);
});

result = listStreamProcessors();
assert.eq(result["ok"], 1, result);
assert.eq(result["streamProcessors"].length, 1, result);
// verify setting invalid feature flag value will terminate a stream processor.
ff = {
    checkpointDuration: {value: false}
};
result =
    db.runCommand({streams_updateFeatureFlags: '', tenantId: TEST_TENANT_ID, featureFlags: ff});
assert.eq(result.ok, false);

assert.soon(() => {
    result = listStreamProcessors();
    assert.eq(result["ok"], 1, result);
    jsTestLog(result);
    return result["streamProcessors"][0]["status"] == "error";
});

stopStreamProcessor(spName);
assert.eq(listStreamProcessors()["streamProcessors"].length, 0);