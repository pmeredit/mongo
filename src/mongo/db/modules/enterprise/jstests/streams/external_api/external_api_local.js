/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {
    TestRESTServer
} from "src/mongo/db/modules/enterprise/jstests/streams/external_api/lib/rest_receiver.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    listStreamProcessors,
    TEST_TENANT_ID
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

// Create a local rest receiver
let restServer = new TestRESTServer();

try {
    restServer.cleanTempFiles();
} catch (err) {
    print("Failed to clean temp files: " + err);
}

restServer.start();

const url = 'http://localhost:' + restServer.getPort();
const webAPIName = "webAPI1";
const inMemoryName = "__testMemory";

let connectionRegistry = [
    {name: inMemoryName, type: 'in_memory', options: {}},
    {name: webAPIName, type: 'web_api', options: {url: url}},
];
const streams = new Streams(TEST_TENANT_ID, connectionRegistry);

testExternalAPIGET();

restServer.cleanTempFiles();
restServer.stop();

function testExternalAPIGET() {
    const spName = "spExternalApi1";
    streams.createStreamProcessor(spName, [
        {$source: {connectionName: inMemoryName}},
        {$externalAPI: {connectionName: webAPIName, as: 'response', urlPath: "/echo/" + spName}},
        {$emit: {connectionName: inMemoryName}},
    ]);
    const sp = streams[spName];
    assert.commandWorked(sp.start({featureFlags: {enableExternalAPIOperator: true}}));

    const result = listStreamProcessors();
    assert.eq(result["ok"], 1, result);
    assert.gte(result["streamProcessors"].length, 1, result);

    assert.commandWorked(db.runCommand(
        {streams_testOnlyInsert: '', tenantId: TEST_TENANT_ID, name: spName, documents: [{}]}));

    const fileName = restServer.getPayloadDirectory() + "/" + spName + "_0.json";
    assert.soon(() => fileExists(fileName));

    const payload = JSON.parse(cat(fileName));

    // Perform a couple cursory checks to ensure data looks valid.
    assert.eq(payload.method, "GET");
    assert.eq(payload.path, "/echo/" + spName);

    assert.commandWorked(sp.stop());
}