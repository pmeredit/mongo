/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    listStreamProcessors,
    TEST_TENANT_ID
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

const dbName = "db";
const inCollName = "inColl";
const outputCollName = "outputColl";
const connectionName = "conn1";
const processorName = "sp1";

const uri = 'mongodb://' + db.getMongo().host;
let connectionRegistry = [{name: connectionName, type: 'atlas', options: {uri: uri}}];
const sp = new Streams(TEST_TENANT_ID, connectionRegistry);

const validationExpr = {
    $gt: ['$fullDocument._id', 9]
};

let pipeline = [
    {$source: {connectionName: connectionName, db: dbName, coll: inCollName}},
    {$validate: {validator: {$expr: validationExpr}, validationAction: 'dlq'}},
    {$merge: {into: {connectionName: connectionName, db: dbName, coll: outputCollName}}},
];

sp.createStreamProcessor(processorName, pipeline);

const processor = sp[processorName];

const dlqCollName = "validateDLQ";
const options = {
    dlq: {connectionName: connectionName, db: dbName, coll: dlqCollName},
    featureFlags: {},
};
assert.commandWorked(db.runCommand(processor.makeStartCmd(options)));

assert.eq(listStreamProcessors()["streamProcessors"].length, 1);

let listCmd = {streams_listStreamProcessors: '', tenantId: TEST_TENANT_ID, name: processorName};

let result = db.runCommand(listCmd);
assert.eq(result["ok"], 1, result);
assert.eq(result["streamProcessors"].length, 1, result);

const processor2 = "sp2";
sp.createStreamProcessor(processor2, [
    {$source: {connectionName: connectionName, db: dbName, coll: "coll2"}},
    {$merge: {into: {connectionName: connectionName, db: dbName, coll: "outColl2"}}},
]);
assert.commandWorked(db.runCommand(sp[processor2].makeStartCmd(options)));

assert.eq(listStreamProcessors()["streamProcessors"].length, 2);

listCmd = {
    streams_listStreamProcessors: '',
    tenantId: TEST_TENANT_ID,
    processorId: processorName
};
result = db.runCommand(listCmd);
jsTestLog(result);
assert.eq(result["ok"], 1, result);
assert.eq(result["streamProcessors"].length, 1, result);
assert.eq(result["streamProcessors"][0].name, processorName);

listCmd = {
    streams_listStreamProcessors: '',
    tenantId: TEST_TENANT_ID,
    processorId: processor2
};
result = db.runCommand(listCmd);
jsTestLog(result);
assert.eq(result["ok"], 1, result);
assert.eq(result["streamProcessors"].length, 1, result);
assert.eq(result["streamProcessors"][0].name, processor2);

sp[processorName].stop();
sp[processor2].stop();
assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
