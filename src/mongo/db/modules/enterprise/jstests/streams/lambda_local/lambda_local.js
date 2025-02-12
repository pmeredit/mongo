import {getPython3Binary} from "jstests/libs/python.js";
import {
    commonFailureTest,
    commonTest
} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
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
        {$project: {makeARequest: 1, requestID: 1, response: 1, payloadToSend: 1, _stream_meta: 1}},
        {$project: {"_stream_meta.source": 0}},
        {$set: {"_stream_meta.externalFunction.responseTimeMs": 1}},
    ];

    const functionName = "function";

    // Test code here
    commonTest({
        input: [
            {action: "hello", makeARequest: true, requestID: 1},
        ],
        pipeline: [
            {
                $externalFunction:
                    {
                        connectionName: testConstants.awsIAMLambdaConnection, functionName, as:
                        "response"}
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
                $externalFunction:
                    {
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
    try {
        assert.commandWorked(
            db.adminCommand({'configureFailPoint': 'awsUserFunctionError', 'mode': 'alwaysOn'}));
        commonTest({
            input: [
                {makeARequest: true, requestID: 1},
            ],
            pipeline: [
                {
                    $externalFunction:
                        {connectionName: testConstants.awsIAMLambdaConnection, functionName, as:
                        "response"}
                },
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
    } finally {
        assert.commandWorked(
            db.adminCommand({'configureFailPoint': 'awsUserFunctionError', 'mode': 'off'}));
    }
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
} finally {
    // Stop the container
    const stop2_ret = runMongoProgram(
        getPython3Binary(), "-u", lambdaContainer, "-v", "stop", "--container", containerName);
    assert.eq(stop2_ret, 0, "Could not stop containers");
}
