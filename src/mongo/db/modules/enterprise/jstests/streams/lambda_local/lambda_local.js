/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {
    resultsEq,
} from "jstests/aggregation/extras/utils.js";
import {getPython3Binary} from "jstests/libs/python.js";
import {TestHelper} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {
    commonFailureTest,
    commonSinkTest,
    commonTest,
    sanitizeDlqDoc
} from "src/mongo/db/modules/enterprise/jstests/streams/common_test.js";
import {
    test as testConstants
} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";

function createRandomString() {
    const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let result = "";
    for (let i = 0; i < 5; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

const lambdaContainer =
    "src/mongo/db/modules/enterprise/jstests/streams/lambda_local/lambda_container.py";

const containerName = "lambda_container_" + createRandomString();

// Stop any previously running containers to ensure we are running from a clean state
const stop_ret = runMongoProgram(
    getPython3Binary(), "-u", lambdaContainer, "-v", "stop", "--container", containerName);
assert.eq(stop_ret, 0, "Could not stop containers");

try {
    // Build and start the container
    const start_ret = runMongoProgram(
        getPython3Binary(), "-u", lambdaContainer, "-v", "start", "--container", containerName);
    assert.eq(start_ret, 0, "Could not start containers");

    const featureFlags = {enableExternalFunctionOperator: true};

    const projectStages = [
        {$addFields: {_stream_meta: {$meta: "stream"}}},
        {$project: {makeARequest: 1, requestID: 1, response: 1, payloadToSend: 1, _stream_meta: 1}},
        {$project: {"_stream_meta.source": 0}},
        {$set: {"_stream_meta.externalFunction.responseTimeMs": 1}},
    ];

    const functionName = "function";

    // Middle Stage Tests
    commonTest({
        input: [
            {action: "hello", makeARequest: true, requestID: 1},
        ],
        pipeline: [
            {
                $externalFunction: {
                    connectionName: testConstants.awsIAMLambdaConnection,
                    functionName,
                    as: "response",
                }
            },
            ...projectStages,
        ],
        expectedOutput: [{
            "makeARequest": true,
            "requestID": 1,
            "_stream_meta": {
                "externalFunction": {
                    "functionName": functionName,
                    "executedVersion": "",
                    "statusCode": 200,
                    "responseTimeMs": 1
                }
            },
            "response": "\"Hello World!\""
        }],
        useTimeField: false,
        featureFlags
    });
    commonTest({
        input: [
            {payloadToSend: {success: "yes I worked", shouldBeExcludeFromRequest: true,
            randomArray: [1,2,3]}},
        ],
        pipeline: [
            {
                $externalFunction: {
                    connectionName: testConstants.awsIAMLambdaConnection,
                    functionName,
                    as: "response",
                    payload: [
                        { $replaceRoot: { newRoot: "$payloadToSend" } },
                        { $addFields: { sum: { $sum: "$randomArray" }}},
                        { $project: { success: 1, sum: 1 }}
                    ]
                }
            },
            ...projectStages,
        ],
        expectedOutput: [{
            "payloadToSend": {
                "success": "yes I worked",
                "shouldBeExcludeFromRequest": true,
                "randomArray": [1,2,3]
            },
            "_stream_meta": {
                "externalFunction": {
                    "functionName": functionName,
                    "executedVersion": "",
                    "statusCode": 200,
                    "responseTimeMs": 1
                }
            },
            "response": {
                "success": "yes I worked",
                "sum": 6
            }
        }],
        useTimeField: false,
        featureFlags
    });
    commonTest({
        input: [
            {
                payloadToSend: {
                    success: "yes I worked",
                    shouldBeExcludeFromRequest: true,
                    randomArray: [1, 2, 3]
                }
            },
        ],
        pipeline: [
            {
                $externalFunction: {
                    connectionName: testConstants.awsIAMLambdaConnection,
                    functionName,
                    execution: "async",
                }
            },
            ...projectStages,
        ],
        expectedOutput: [{
            "payloadToSend": {
                "success": "yes I worked",
                "shouldBeExcludeFromRequest": true,
                "randomArray": [1, 2, 3]
            },
            "_stream_meta": {
                "externalFunction": {
                    "functionName": functionName,
                    "executedVersion": "",
                    "statusCode": 200,
                    "responseTimeMs": 1
                }
            },
        }],
        useTimeField: false,
        featureFlags
    });

    // Sink Stage Tests
    commonSinkTest({
        input: [
            {action: "hello", makeARequest: true, requestID: 1},
        ],
        pipeline: [
            {
                $externalFunction: {
                    connectionName: testConstants.awsIAMLambdaConnection,
                    functionName,
                }
            },
        ],
        expectedOutputMessageCount: 1,
        expectedDlq: [],
        useTimeField: false,
        featureFlags
    });
    commonSinkTest({
        input: [
            {
                payloadToSend: {
                    success: "yes I worked",
                    shouldBeExcludeFromRequest: true,
                    randomArray: [1, 2, 3]
                }
            },
        ],
        pipeline: [
            {
                $externalFunction: {
                    connectionName: testConstants.awsIAMLambdaConnection,
                    functionName,
                    payload: [
                        {$replaceRoot: {newRoot: "$payloadToSend"}},
                        {$addFields: {sum: {$sum: "$randomArray"}}},
                        {$project: {success: 1, sum: 1}}
                    ]
                }
            },
        ],
        expectedOutputMessageCount: 1,
        expectedDlq: [],
        useTimeField: false,
        featureFlags
    });
    commonSinkTest({
        input: [
            {
                payloadToSend: {
                    success: "yes I worked",
                    shouldBeExcludeFromRequest: true,
                    randomArray: [1, 2, 3]
                }
            },
        ],
        pipeline: [
            {
                $externalFunction: {
                    connectionName: testConstants.awsIAMLambdaConnection,
                    functionName,
                    execution: "async",
                }
            },
        ],
        expectedOutputMessageCount: 1,
        expectedDlq: [],
        useTimeField: false,
        featureFlags
    });

    // DLQ Tests
    try {
        assert.commandWorked(
            db.adminCommand({'configureFailPoint': 'awsUserFunctionError', 'mode': 'alwaysOn'}));
        commonTest({
            input: [
                {makeARequest: true, requestID: 1},
            ],
            pipeline: [
                {
                    $externalFunction: {
                        connectionName: testConstants.awsIAMLambdaConnection,
                        functionName, 
                        as: "response",
                    }
                },
                ...projectStages,
            ],
            expectedOutput: [],
            expectedDlq: [
                {
                    "errInfo" : {
                        "reason" : "Failed to process input document in $externalFunction with error: Request failure in $externalFunction with error: Received error response from external function. Payload: Uncaught User Exception"
                    },
                    "operatorName" : "ExternalFunctionOperator",
                    "doc" : {
                        "makeARequest" : true,
                        "requestID" : 1,
                    }
                }
            ],
            useTimeField: false,
            featureFlags
        });

        commonSinkTest({
            input: [
                {makeARequest: true, requestID: 1},
            ],
            pipeline: [
                {
                    $externalFunction: {
                        connectionName: testConstants.awsIAMLambdaConnection,
                        functionName,
                    }
                },
            ],
            expectedOutputMessageCount: 0,
            expectedDlq: [{
                "errInfo": {
                    "reason":
                        "Failed to process input document in $externalFunction with error: Request failure in $externalFunction with error: Received error response from external function. Payload: Uncaught User Exception"
                },
                "operatorName": "ExternalFunctionSinkOperator",
                "doc": {
                    "makeARequest": true,
                    "requestID": 1,
                }
            }],
            useTimeField: false,
            featureFlags
        });
    } finally {
        assert.commandWorked(
            db.adminCommand({'configureFailPoint': 'awsUserFunctionError', 'mode': 'off'}));
    }

    // Failure Tests
    try {
        assert.commandWorked(
            db.adminCommand({'configureFailPoint': 'awsLambdaNotFound', 'mode': 'alwaysOn'}));
        commonFailureTest({
            input: [
                {makeARequest: true, requestID: 1},
            ],
            pipeline: [
                {
                    $externalFunction:
                        {connectionName: testConstants.awsIAMLambdaConnection, functionName, as: "response"}
                },
            ],
            expectedErrorCode: 9929406,
            useTimeField: false,
            featureFlags
        });
    } finally {
        assert.commandWorked(
            db.adminCommand({'configureFailPoint': 'awsLambdaNotFound', 'mode': 'off'}));
    }
    try {
        assert.commandWorked(db.adminCommand(
            {'configureFailPoint': 'awsLambdaBadRequestContent', 'mode': 'alwaysOn'}));
        commonFailureTest({
            input: [
                {makeARequest: true, requestID: 1},
            ],
            pipeline: [
                {
                    $externalFunction:
                        {connectionName: testConstants.awsIAMLambdaConnection, functionName, as: "response"}
                },
            ],
            expectedErrorCode: ErrorCodes.StreamProcessorExternalFunctionConnectionError,
            useTimeField: false,
            featureFlags
        });
    } finally {
        assert.commandWorked(
            db.adminCommand({'configureFailPoint': 'awsLambdaBadRequestContent', 'mode': 'off'}));
    }
    awsLambdaUpdateConnectionTest({
        input: [
            {
                payloadToSend: {
                    success: "yes I worked",
                    shouldBeExcludeFromRequest: true,
                    randomArray: [1, 2, 3]
                }
            },
        ],
        pipeline: [
            {
                $externalFunction: {
                    connectionName: testConstants.awsIAMLambdaConnection,
                    functionName,
                    execution: "async",
                }
            },
        ],
        expectedOutputMessageCount: 1,
        expectedDlq: [],
        useTimeField: false,
        featureFlags
    });
} finally {
    // Stop the container
    const stop2_ret = runMongoProgram(
        getPython3Binary(), "-u", lambdaContainer, "-v", "stop", "--container", containerName);
    assert.eq(stop2_ret, 0, "Could not stop containers");
}

function awsLambdaUpdateConnectionTest({
    input,
    pipeline,
    expectedOutputMessageCount,
    expectedDlq,
    useTimeField = true,
    featureFlags = {},
}) {
    // Test with a changestream source and a checkpoint in the middle.
    const newPipeline = [
        {$match: {operationType: "insert"}},
        {$replaceRoot: {newRoot: "$fullDocument"}},
        ...pipeline,
    ];
    const test = new TestHelper(
        input,
        newPipeline,
        undefined,  // interval
        "atlas",    // sourcetype
        undefined,  // useNewCheckpointing
        undefined,  // useRestoredExecutionPlan
        undefined,  // writeDir
        undefined,  // restoreDir
        undefined,  // dbForTest
        undefined,  // targetSourceMergeDb
        useTimeField,
        "included",  // sinkType
    );
    test.startOptions.featureFlags = Object.assign(test.startOptions.featureFlags, featureFlags);

    jsTestLog(`Running with input length ${input.length}, first doc ${input[tojson(0)]}`);

    test.run();
    assert.commandWorked(test.inputColl.insertMany(input));

    var waitTimeMs = 1000 * 10;

    assert.soon(() => { return test.stats()["inputMessageCount"] == input.length; },
                tojson(test.stats()),
                waitTimeMs);

    const waitForCount = expectedOutputMessageCount + expectedDlq.length;
    assert.soon(() => {
        return test.stats()["outputMessageCount"] + test.stats()["dlqMessageCount"] == waitForCount;
    }, tojson(test.stats()), waitTimeMs);

    assert.eq(test.stats()["outputMessageCount"], expectedOutputMessageCount);
    assert.eq(test.stats()["dlqMessageCount"], expectedDlq.length);
    assert.soon(() => {
        return resultsEq(test.dlqColl.find({}).toArray().map(d => sanitizeDlqDoc(d)),
                         expectedDlq,
                         true /* verbose */,
                         ["_id"]);
    });

    assert.commandWorked(db.runCommand({
        streams_updateConnection: '',
        tenantId: test.tenantId,
        processorId: test.processorId,
        processorName: test.spName,
        connection: {
            name: "StreamsAWSIAMLambdaConnection",
            type: 'aws_iam_lambda',
            options: {
                accessKey: "newAccessKey",
                accessSecret: "newAccessSecret",
                sessionToken: "newSessionToken",
                expirationDate: new Date(Date.now() + (1000 * 600))  // 10 minutes from now
            }
        },
    }));

    test.stop();
}
