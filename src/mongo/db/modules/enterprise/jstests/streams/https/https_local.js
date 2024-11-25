/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {
    resultsEq,
} from "jstests/aggregation/extras/utils.js";
import {uuidStr} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    InsufficientCurlVersionError,
    TestRESTServer
} from "src/mongo/db/modules/enterprise/jstests/streams/https/lib/rest_receiver.js";
import {
    listStreamProcessors,
    TEST_TENANT_ID
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

function runTest({
    spName,
    dbConnectionName,
    httpsOptions,
    inputDocs,
    expectedRequests = [],
    outputQuery,
    expectedOutput = [],
    expectedDlq = [],
    fieldsToSkip = [],
    featureFlags = {},
    expectedStatus = "running",
}) {
    const waitTimeMs = 30000;
    const dbName = "https_test";
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
        {$https: httpsOptions},
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

    if (expectedStatus != "running") {
        assert.soon(() => { return sp.stats()["status"] == expectedStatus; },
                    "waiting for expected status",
                    waitTimeMs);
        return;
    }

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

    let allFieldsToSkip = ["_id", "responseTimeMs"];
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
        output.forEach((out, i) => {
            const responseTimeMs = out['_stream_meta']['https']['responseTimeMs'];
            assert(responseTimeMs >= 0);
            jsTestLog(`RESPONSE TIME MS: ${responseTimeMs}`);
        });
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
        assert.soon(() => fileExists(fileName),
                    `waiting to find rest server logged request file: ${fileName}`);

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

try {
    restServer.tryStart();
} catch (err) {
    if (err === InsufficientCurlVersionError) {
        print("Skipping test. Host has insufficient curl version.");
        quit();
    }
    throw err;
}

const restServerUrl = 'http://localhost:' + restServer.getPort();
const httpsName = "https1";
const httpsNameWithTrailingSlash = "https2";
const dbConnectionName = "db1";
const mongoUrl = 'mongodb://' + db.getMongo().host;

const connectionRegistry = [
    {name: dbConnectionName, type: 'atlas', options: {uri: mongoUrl}},
    {name: httpsName, type: 'https', options: {url: restServerUrl}},
    {name: httpsNameWithTrailingSlash, type: 'https', options: {url: restServerUrl + "/"}},
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
        httpsOptions:
            {connectionName: httpsName, path: "/echo/tc1", method: "GET", as:
            'response'},
        inputDocs: [{a: 1}],
        expectedRequests:
            [{method: "GET", path: "/echo/tc1", headers: basicHeaders, query: {}, body: ""}],
        outputQuery: [{$project: {fullDocument: 1, response: 1, "_stream_meta.https": 1}}],
        expectedOutput: [{
            fullDocument: {a: 1},
            _stream_meta: {https: {
                url: restServerUrl + "/echo/tc1",
                method: "GET",
                httpStatusCode: 200,
            }},
            response: {method: "GET", path: "/echo/tc1", headers: basicHeaders, query: {}, body:
            ""}
        }],
        allowAllTraffic: true,
    },
    {
        description: "post request with additional headers should be made with body and should send a payload and save response to the as field",
        spName: "tc2",
        httpsOptions:
            {connectionName: httpsName, path: "/echo/tc2", method: "POST", as:
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
                "_stream_meta.https": 1,
            },
        }],
        expectedOutput: [{
            fullDocument: {a: 1},
            _stream_meta: {https: {
                url: restServerUrl + "/echo/tc2",
                method: "POST",
                httpStatusCode: 200,
            }},
            response: {inner: {method: "POST", path: "/echo/tc2", headers: {...basicHeaders,
            "Content-Length" : "619", "Content-Type": "application/json"}, query: {},
            body: {fullDocument: {a: 1}}}}
        }],
        allowAllTraffic: true,
    },
    {
        description: "error responses should prompt DLQ messages to be made by default",
        spName: "tcOnErrorDefault",
        httpsOptions:
            {connectionName: httpsName, path: "/notfound/tcOnErrorDefault", method: "GET", as: 'response'},
        inputDocs: [{a: 1}],
        allowAllTraffic: true,
        expectedDlq: [{a: 1}],
    },
    {
        description: "error responses should prompt DLQ messages to be made when configured to do so",
        spName: "tcOnErrorDLQ",
        httpsOptions:
            {connectionName: httpsName, path: "/notfound/tcOnErrorDLQ", method: "GET", as: 'response', onError: "dlq"},
        inputDocs: [{a: 1}],
        expectedDlq: [{a: 1}],
    },
    {
        description: "error responses should put SP in error state when configured to do so",
        spName: "tcOnErrorFail",
        httpsOptions:
            {connectionName: httpsName, path: "/notfound/tcOnErrorFail", method: "GET", as:
            'response', onError: "fail"},
        inputDocs: [{a: 1}],
        allowAllTraffic: true,
        expectedStatus: "error",
    },
    {
        description: "error responses should be ignored when configured to do so",
        spName: "tcOnErrorIgnore",
        httpsOptions:
            {connectionName: httpsName, path: "/notfound/tcOnErrorIgnore", method: "GET", as:
            'response', onError: "ignore"},
        inputDocs: [{a: 1}],
        allowAllTraffic: true,
        expectedRequests:
            [{method: "GET", path: "/notfound/tcOnErrorIgnore", headers: basicHeaders, query: {}, body: ""}],
        outputQuery: [{$project: {fullDocument: 1, response: 1, "_stream_meta.https": 1}}],
        expectedOutput: [{
            fullDocument: {a: 1},
            _stream_meta: {https: {
                url: restServerUrl + "/notfound/tcOnErrorIgnore",
                method: "GET",
                httpStatusCode: 404,
            }},
        }],
    },
    {
        description: "put request with added headers should work",
        spName: "tc3",
        httpsOptions: {
            connectionName: httpsName,
            path: "/echo/tc3",
            method: "PUT",
            as: 'response',
            headers: {
                "FieldPathHeader": "$fullDocument.foo",
                "StrHeader": "foo"
            },
        },
        inputDocs: [{a: 1, foo: "DynamicValue"}],
        outputQuery: [{$project: {
            fullDocument: 1,
            "response.method": 1,
            "response.path": 1,
            "response.headers": 1,
            "_stream_meta.https": 1,
        }}],
        expectedOutput: [{
            fullDocument: {a: 1, foo: "DynamicValue"},
            _stream_meta: {https: {
                url: restServerUrl + "/echo/tc3",
                method: "PUT",
                httpStatusCode: 200,
            }},
            response: {
                method: "PUT",
                path: "/echo/tc3",
                headers: {
                        ...basicHeaders,
                        "Content-Type": "application/json",
                        "Content-Length" : "640",
                        "FieldPathHeader": "DynamicValue",
                        "StrHeader": "foo"
                }
            }
        }],
        fieldsToSkip: ["Expect"],
        allowAllTraffic: true
    },
    {
        description: "put request with added query parameter should work",
        spName: "tc4",
        httpsOptions: {
            connectionName: httpsName,
            path: "/echo/tc4",
            method: "PATCH",
            as: 'response',
            parameters: {
                "StrParam": "StaticParameterValue",
                "DoubleParam": 1.100000000002,
                "FieldPathExprParam": "$fullDocument.foo",
                "ObjectExprParam": {
                    "$sum": [1.2, 2, 3]
                },
                "BoolParam": true,
                "SearchParam": "\"%!:+-.@/foobar baz\""
            }
        },
        inputDocs: [{a: 1, foo: "DynamicValue"}],
        outputQuery: [{
            $project: {
                fullDocument: 1,
                "response.method": 1,
                "response.path": 1,
                "response.query": 1,
                "response.headers": 1,
                "_stream_meta.https": 1,
            }
        }],
        expectedOutput: [{
            fullDocument: {a: 1, foo: "DynamicValue"},
            _stream_meta: {https: {
                url: restServerUrl + "/echo/tc4?StrParam=StaticParameterValue&DoubleParam=1.100000000002&FieldPathExprParam=DynamicValue&ObjectExprParam=6.2&BoolParam=true&SearchParam=%22%25%21%3a%2b-.%40%2ffoobar%20baz%22",
                method: "PATCH",
                httpStatusCode: 200,
            }},
            response: {
                method: "PATCH",
                path: "/echo/tc4",
                headers: {
                    ...basicHeaders,
                    "Content-Length" : "640",
                    "Content-Type" : "application/json"
                },
                query: {
                    "StrParam": ["StaticParameterValue"],
                    "DoubleParam": ["1.100000000002"],
                    "FieldPathExprParam": ["DynamicValue"],
                    "ObjectExprParam": ["6.2"],
                    "BoolParam": ["true"],
                    "SearchParam": ["\"%!:+-.@/foobar baz\""]
                },
            }
        }],
        fieldsToSkip: ["Expect"],
        allowAllTraffic: true,
    },
    {
        description: "get request with valid params and headers",
        spName: "paramsAndHeadersAreValid",
        httpsOptions: {            
            connectionName: httpsName,
            as: 'response',
            path: "/foo(bar)",
            method: "GET",
            headers: {
                "FieldPathHeader": "$fullDocument.foo",
                "StrHeader": "foo"
            },
            parameters: {
                "StrParam": "StaticParameterValue",
                "DoubleParam": 1.100000000002,
                "FieldPathExprParam": "$fullDocument.foo",
                "ObjectExprParam": {
                    "$sum": [1.2, 2, 3]
                },
                "BoolParam": true,
                "Search%Param": "https://user:password@my.domain.net:1234/foo/bar/baz?name=hero&name=sandwich&name=grinder#heading1"
            }
        },
        outputQuery: [{
            $project: {
                fullDocument: 1,
                "response.method": 1,
                "response.path": 1,
                "response.query": 1,
                "response.headers": 1,
                "response.body.fullDocument": 1,
                "_stream_meta.https": 1,
            }
        }],
        expectedRequests: [],
        inputDocs: [{foo: "DynamicValue"}],
        expectedOutput: [{
                fullDocument: {foo: "DynamicValue"},
                _stream_meta: {https: {
                    url: restServerUrl + "/foo%28bar%29?StrParam=StaticParameterValue&DoubleParam=1.100000000002&FieldPathExprParam=DynamicValue&ObjectExprParam=6.2&BoolParam=true&Search%25Param=https%3a%2f%2fuser%3apassword%40my.domain.net%3a1234%2ffoo%2fbar%2fbaz%3fname%3dhero%26name%3dsandwich%26name%3dgrinder%23heading1",
                    method: "GET",
                    httpStatusCode: 200,
                }},
                response: {
                    method: "GET",
                    path: "/foo%28bar%29",
                    query: {
                        "StrParam": ["StaticParameterValue"],
                        "DoubleParam": ["1.100000000002"],
                        "FieldPathExprParam": ["DynamicValue"],
                        "ObjectExprParam": ["6.2"],
                        "BoolParam": ["true"],
                        "Search%Param": ["https://user:password@my.domain.net:1234/foo/bar/baz?name=hero&name=sandwich&name=grinder#heading1"]
                    },
                    headers: {
                        ...basicHeaders,
                        "FieldPathHeader": "DynamicValue",
                        "StrHeader" : "foo",
                    }
                }
        }],
        allowAllTraffic: true,
    },
    {
        description: "get request that receives a plain text response",
        spName: "plainTextResponse",
        httpsOptions:
            {connectionName: httpsName, path: "/plaintext/plainTextResponse", method: "GET", as:
            'response'},
        inputDocs: [{a: 1}],
        expectedRequests:
            [{method: "GET", path: "/plaintext/plainTextResponse", headers: basicHeaders, query: {}, body: ""}],
        outputQuery: [{$project: {fullDocument: 1, response: 1, "_stream_meta.https": 1}}],
        expectedOutput: [{
            fullDocument: {a: 1},
            _stream_meta: {https: {
                url: restServerUrl + "/plaintext/plainTextResponse",
                method: "GET",
                httpStatusCode: 200,
            }},
            response: "A_VALID_PLAINTEXT_RESPONSE",
        }],
        allowAllTraffic: true,
    },
    {
        description: "post request with inner pipeline should send a payload and save response to the as field",
        spName: "payloadPipeline",
        httpsOptions:
            {connectionName: httpsName, path: "/echo/payloadPipeline", method: "POST", as:
            'response', payload: [{$replaceRoot: { newRoot: "$fullDocument.payloadToSend" } }, { $addFields: { sum: { $sum: "$randomArray" }}}, { $project: { success: 1, sum: 1 }} ]},
        inputDocs: [{payloadToSend: {success: "yes I worked", shouldBeExcludeFromRequest: true, randomArray: [1,2,3]}}],
        expectedRequests:
            [{method: "POST", path: "/echo/payloadPipeline", headers: {...basicHeaders,
                "Content-Type": "application/json", "Content-Length" : "34"}, query: {}, body: {success: "yes I worked", sum: 6}}],
        outputQuery: [{
            $project: {
                fullDocument: 1,
                "response.method": 1,
                "response.path": 1,
                "response.headers": 1,
                "response.query": 1,
                "response.body": 1,
                "_stream_meta.https": 1,
            }
        }],
        expectedOutput: [{
            fullDocument: {payloadToSend: {success: "yes I worked", shouldBeExcludeFromRequest: true, randomArray: [1,2,3]}},
            response: {method: "POST", path: "/echo/payloadPipeline", headers: {...basicHeaders,
            "Content-Length" : "34", "Content-Type": "application/json"}, query: {},
            body: {success: "yes I worked", sum: 6}}
        }],
        allowAllTraffic: true,
    },
    {
        description: "http client should block request and send it to the dlq based on override feature flag",
        spName: "tcFirewallBlockedRequestFF",
        httpsOptions:
            {connectionName: httpsName, path: "/echo/tcFirewallBlockedRequestFF", method: "GET", as: 'response'},
        inputDocs: [{a: 1}],
        expectedDlq: [{a: 1}],
        cidrDenyList: ["127.0.0.1/32"]
    },
    {
        description: "http client should block request and send it to the dlq based on default feature flag",
        spName: "tcFirewallBlockedRequestDefault",
        httpsOptions:
            {connectionName: httpsName, path: "/echo/tcFirewallBlockedRequestDefault", method: "GET", as: 'response'},
        inputDocs: [{a: 1}],
        expectedDlq: [{a: 1}],
    },
    {
        description: "GET with path to evaluate should make a successful request",
        spName: "evaluatedPath",
        httpsOptions:
            {connectionName: httpsName, path: "$fullDocument.foo", method: "GET", as:
            'response'},
        inputDocs: [{foo: "/echo/evaluatedPath"}],
        expectedRequests:
            [{method: "GET", path: "/echo/evaluatedPath", headers: basicHeaders, query: {}, body: ""}],
        outputQuery: [{$project: {fullDocument: 1, response: 1, "_stream_meta.https": 1}}],
        expectedOutput: [{
            fullDocument: {foo: "/echo/evaluatedPath"},
            response: {method: "GET", path: "/echo/evaluatedPath", headers: basicHeaders, query: {}, body:
            ""}
        }],
        allowAllTraffic: true,
    },
    {
        description: "GET with trailing slash should make request",
        spName: "trailingSlash",
        httpsOptions:
            {connectionName: httpsNameWithTrailingSlash, method: "GET", path: "/echo/trailingSlash/", as:
            'response'},
        inputDocs: [{foo: "bar"}],
        expectedRequests:
            [{method: "GET", path: "/echo/trailingSlash", headers: basicHeaders, query: {}, body: ""}],
        outputQuery: [{$project: {fullDocument: 1, response: 1, "_stream_meta.https": 1}}],
        expectedOutput: [{
            fullDocument: {foo: "bar"},
            response: {method: "GET", path: "/echo/trailingSlash", headers: basicHeaders, query: {}, body:
            ""}
        }],
        allowAllTraffic: true,
    },
];

for (const tc of testCases) {
    tc.dbConnectionName = dbConnectionName;
    tc.featureFlags = {enableHttpsOperator: true};
    if (tc.allowAllTraffic) {
        tc.featureFlags.cidrDenyList = [];
    }
    if (tc.cidrDenyList) {
        tc.featureFlags.cidrDenyList = tc.cidrDenyList;
    }
    jsTestLog(`Running: ${tojson(tc)}`);
    runTest({...tc});
}

restServer.cleanTempFiles();
restServer.stop();
