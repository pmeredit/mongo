/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {
    resultsEq,
} from "jstests/aggregation/extras/utils.js";
import {uuidStr} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {
    TestRESTServer
} from "src/mongo/db/modules/enterprise/jstests/streams/external_api/lib/rest_receiver.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    listStreamProcessors,
    TEST_TENANT_ID
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

function runTest({
    spName,
    dbConnectionName,
    externalAPIOptions,
    inputDocs,
    expectedRequests = [],
    outputQuery,
    expectedOutput = [],
    expectedDlq = [],
    fieldsToSkip = [],
    featureFlags = {},
}) {
    const waitTimeMs = 30000;
    const dbName = "external_api_test";
    const dlqCollName = "dlq_" + uuidStr();
    const inputCollName = "input_" + uuidStr();
    const outputCollName = "output_" + uuidStr();

    const outputColl = db.getSiblingDB(dbName)[outputCollName];
    outputColl.drop();
    const inputColl = db.getSiblingDB(dbName)[inputCollName];
    const dlqColl = db.getSiblingDB(dbName)[dlqCollName];

    streams.createStreamProcessor(spName, [
        {
            $source: {
                connectionName: dbConnectionName,
                db: dbName,
                coll: inputCollName,
                config: {startAtOperationTime: db.hello().$clusterTime.clusterTime}
            }
        },
        {$externalAPI: externalAPIOptions},
        {
            $merge: {
                into: {connectionName: dbConnectionName, db: dbName, coll: outputCollName},
            }
        },
    ]);
    const sp = streams[spName];
    assert.commandWorked(sp.start({
        dlq: {connectionName: dbConnectionName, db: dbName, coll: dlqCollName},
        featureFlags: featureFlags,
    }));

    const result = listStreamProcessors();
    assert.eq(result["ok"], 1, result);
    assert.gte(result["streamProcessors"].length, 1, result);

    assert.soon(
        () => { return sp.stats()["status"] == "running"; }, tojson(sp.stats()), waitTimeMs);

    // evg timing is flakey for doc insert
    assert.commandWorked(inputColl.insertMany(inputDocs));
    try {
        // Wait for all the messages to be read.
        assert.soon(() => { return sp.stats()["inputMessageCount"] == inputDocs.length; },
                    tojson(sp.stats()),
                    waitTimeMs,
                    null,
                    {runHangAnalyzer: false});
    } catch {
        if (sp.stats()["inputMessageCount"] == 0) {
            // we failed to notice the inserted docs try again
            assert.commandWorked(inputColl.insertMany(inputDocs));
            assert.soon(() => { return sp.stats()["inputMessageCount"] == inputDocs.length; },
                        tojson(sp.stats()),
                        waitTimeMs);
        }
    }

    // wait for docs to pass to output and dlq.
    assert.soon(() => {
        return sp.stats()["outputMessageCount"] + sp.stats()["dlqMessageCount"] ==
            expectedOutput.length + expectedDlq.length;
    }, "waiting for expected output", waitTimeMs);

    assert.eq(sp.stats()["outputMessageCount"], expectedOutput.length);
    assert.eq(sp.stats()["dlqMessageCount"], expectedDlq.length);

    let allFieldsToSkip = ["_id"];
    if (fieldsToSkip) {
        allFieldsToSkip.push(...fieldsToSkip);
    }

    if (expectedOutput.length > 0) {
        let output;
        assert.soon(() => {
            output = outputColl.aggregate(outputQuery).toArray();
            return output.length == expectedOutput.length;
        }, "waiting for expected output in collection", waitTimeMs);
        jsTestLog(output);
        assert(resultsEq(expectedOutput, output, true /* verbose */, allFieldsToSkip));
    }
    if (expectedDlq.length > 0) {
        // Validate we see the expected results in the DLQ collection.
        let dlqOutput;
        assert.soon(() => {
            dlqOutput = dlqColl
                            .aggregate([
                                {$replaceRoot: {newRoot: "$doc"}},
                                {$replaceRoot: {newRoot: "$fullDocument"}}
                            ])
                            .toArray();
            return dlqOutput.length == expectedDlq.length;
        }, "waiting for expected dlq in collection", waitTimeMs);
        jsTestLog(dlqOutput);
        assert(resultsEq(expectedDlq, dlqOutput, true /* verbose */, allFieldsToSkip));
    }

    for (let i = 0; i < expectedRequests.length; i++) {
        const fileName = restServer.getPayloadDirectory() + "/" + spName + "_" + i + ".json";
        assert.soon(() => fileExists(fileName));

        const payload = JSON.parse(cat(fileName));
        assert.eq(payload.method, expectedRequests[i].method);
        assert.eq(payload.path, expectedRequests[i].path);
        assert.eq(payload.body, expectedRequests[i].body);

        if (expectedRequests[i].hasOwnProperty("query")) {
            assert.eq(payload.query, expectedRequests[i].query);
        }
        if (expectedRequests[i].hasOwnProperty("headers")) {
            assert.eq(payload.headers, expectedRequests[i].headers);
        }
    }

    assert.commandWorked(sp.stop());
    inputColl.drop();
    outputColl.drop();
    dlqColl.drop();
}

// Create a local rest receiver
const restServer = new TestRESTServer();
try {
    restServer.cleanTempFiles();
} catch (err) {
    print("Failed to clean temp files: " + err);
}
restServer.start();

const restServerUrl = 'http://localhost:' + restServer.getPort();
const webAPIName = "webAPI1";
const dbConnectionName = "db1";
const mongoUrl = 'mongodb://' + db.getMongo().host;

const connectionRegistry = [
    {name: dbConnectionName, type: 'atlas', options: {uri: mongoUrl}},
    {name: webAPIName, type: 'web_api', options: {url: restServerUrl}},
];
const streams = new Streams(TEST_TENANT_ID, connectionRegistry);

const basicHeaders = {
    "Host": 'localhost:' + restServer.getPort(),
    "Accept": "*/*",
    "Connection": "keep-alive",
};

const testCases = [
    {
        description: "get request should be made without query or body and should and save response to the as field",
        spName: "tc1",
        externalAPIOptions:
            {connectionName: webAPIName, urlPath: "/echo/tc1", requestType: "GET", as:
            'response'},
        inputDocs: [{a: 1}],
        expectedRequests:
            [{method: "GET", path: "/echo/tc1", headers: basicHeaders, query: {}, body: ""}],
        outputQuery: [{$project: {fullDocument: 1, response: 1}}],
        expectedOutput: [{
            fullDocument: {a: 1},
            response: {method: "GET", path: "/echo/tc1", headers: basicHeaders, query: {}, body:
            ""}
        }],
        allowAllTraffic: true,
    },
    {
        description: "post request with additional headers should be made with body and should send a payload and save response to the as field",
        spName: "tc2",
        externalAPIOptions:
            {connectionName: webAPIName, urlPath: "/echo/tc2", requestType: "POST", as:
            'response.inner'},
        inputDocs: [{a: 1}],
        outputQuery: [{
            $project: {
                fullDocument: 1,
                "response.inner.method": 1,
                "response.inner.path": 1,
                "response.inner.headers": 1,
                "response.inner.query": 1,
                "response.inner.body.fullDocument": 1,
            }
        }],
        expectedOutput: [{
            fullDocument: {a: 1},
            response: {inner: {method: "POST", path: "/echo/tc2", headers: {...basicHeaders,
            "Content-Length" : "626", "Content-Type": "application/json"}, query: {},
            body: {fullDocument: {a: 1}}}}
        }],
        allowAllTraffic: true,
    },
    // TODO(SERVER-95031): uncomment test cases
    // {
    //     spName: "tc3",
    //     externalAPIOptions:
    //         {connectionName: webAPIName, urlPath: "/echo/tc3", requestType: "PUT", as:
    //         'response', headers: {"option-header": "test"}},
    //     inputDocs: [{a: 1}],
    //     outputQuery: [{$project: {fullDocument: 1, response: 1}}],
    //     expectedOutput: [{
    //         fullDocument: {a: 1},
    //         response: {inner: {method: "PUT", path: "/echo/tc3", headers: {...basicHeaders,
    //         "Content-Length" : "626", "option-header": "test"}, query: {},
    //         body: {fullDocument: {a: 1}}}}
    //     }],
    // },
    // {
    //     spName: "tc4",
    //     externalAPIOptions:
    //         {connectionName: webAPIName, urlPath: "/echo/tc4", requestType: "PATCH", as:
    //         'response', parameters: {"option-param": true}},
    //     inputDocs: [{a: 1}],
    //     outputQuery: [{$project: {fullDocument: 1, response: 1}}],
    //     expectedOutput: [{
    //         fullDocument: {a: 1},
    //         response: {inner: {method: "PATCH", path: "/echo/tc4", headers: {...basicHeaders,
    //         "Content-Length" : "626"}, query: {"option-param": true},
    //         body: {fullDocument: {a: 1}}}}
    //     }],
    // },
    {
        description: "http client should block request and send it to the dlq based on override feature flag",
        spName: "tcFirewallBlockedRequestFF",
        externalAPIOptions:
            {connectionName: webAPIName, urlPath: "/echo/tcFirewallBlockedRequestFF", requestType: "GET", as: 'response'},
        inputDocs: [{a: 1}],
        expectedDlq: [{a: 1}],
        cidrDenyList: ["127.0.0.1/32"]
    },
    {
        description: "http client should block request and send it to the dlq based on default feature flag",
        spName: "tcFirewallBlockedRequestDefault",
        externalAPIOptions:
            {connectionName: webAPIName, urlPath: "/echo/tcFirewallBlockedRequestDefault", requestType: "GET", as: 'response'},
        inputDocs: [{a: 1}],
        expectedDlq: [{a: 1}],
    },
];

for (const tc of testCases) {
    tc.dbConnectionName = dbConnectionName;
    tc.featureFlags = {enableExternalAPIOperator: true};
    if (tc.allowAllTraffic) {
        tc.featureFlags.cidrDenyList = [];
    }
    if (tc.cidrDenyList) {
        tc.featureFlags.cidrDenyList = tc.cidrDenyList;
    }
    jsTestLog(`Running: ${tojson(tc)}`);
    runTest(tc);
}

restServer.cleanTempFiles();
restServer.stop();
