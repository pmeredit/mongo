import {getPython3Binary} from "jstests/libs/python.js";
import {
    commonFailureTest,
    commonSinkTest,
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

const containerHarness = "src/mongo/db/modules/enterprise/jstests/streams/s3_local/s3_container.py";

const containerName = "s3_container_" + createRandomString();
const knownS3Bucket = "jstest";

try {
    // Build and start the container
    const start_ret = runMongoProgram(
        getPython3Binary(), "-u", containerHarness, "-v", "start", "--container", containerName);
    assert.eq(start_ret, 0, "Could not start containers");

    const featureFlags = {enableS3Emit: true};

    try {
        // Happy Path. S3 $emit stage should successfully write to S3.
        commonSinkTest({
            input: [
                {message: "hello world"},
            ],
            pipeline: [
                {
                    $emit: {
                        connectionName: testConstants.awsIAMS3Connection,
                        bucket: knownS3Bucket,
                        region: "us-east-1",
                        path: "mypath",
                        delimiter: "\n",
                    }
                },
            ],
            expectedOutputMessageCount: 1,
            expectedDlq: [],
            useTimeField: false,
            featureFlags,
        });
    } finally {
        jsTestLog("Cleaning bucket");
        const returnCode = runMongoProgram(getPython3Binary(),
                                           "-u",
                                           containerHarness,
                                           "-v",
                                           "clean_bucket",
                                           "--container",
                                           containerName);
        assert.eq(returnCode, 0, "Failed to clean bucket");
    }

    // Should fail when provided an invalid region
    commonFailureTest({
        input: [
            {message: "hello world"},
        ],
        pipeline: [
            {
                $emit: {
                    connectionName: testConstants.awsIAMS3Connection,
                    bucket: knownS3Bucket,
                    region: "mars-east-1",  // This is an invalid region
                    path: "mypath",
                    delimiter: "\n",
                }
            },
        ],
        expectedErrorCode: 420,
        useTimeField: false,
        featureFlags,
        failsOnStartup: true,
        sinkType: "included",
    });

    // Should fail when bucket (provided as static string) is not found during connection
    // validation.
    commonFailureTest({
        input: [
            {message: "hello world"},
        ],
        pipeline: [
            {
                $emit: {
                    connectionName: testConstants.awsIAMS3Connection,
                    bucket: "unknownBucket",  // This bucket does not exist.
                    region: "us-east-1",
                    path: "mypath",
                    delimiter: "\n",
                }
            },
        ],
        expectedErrorCode: 420,
        useTimeField: false,
        featureFlags,
        failsOnStartup: true,
        sinkType: "included",
    });

    // Should fail when path (provided as a static string) is invalid. The SP should fail upon
    // starting.
    commonFailureTest({
        input: [
            {message: "hello world"},
        ],
        pipeline: [
            {
                $emit: {
                    connectionName: testConstants.awsIAMS3Connection,
                    bucket: knownS3Bucket,
                    region: "us-east-1",
                    path: "my`invalid[path]",  // This key contains invalid characters.
                    delimiter: "\n",
                }
            },
        ],
        expectedErrorCode: 420,
        useTimeField: false,
        featureFlags,
        failsOnStartup: true,
        sinkType: "included",
    });
} finally {
    // Stop the container
    const stop2_ret = runMongoProgram(
        getPython3Binary(), "-u", containerHarness, "-v", "stop", "--container", containerName);
    assert.eq(stop2_ret, 0, "Could not stop containers");
}
