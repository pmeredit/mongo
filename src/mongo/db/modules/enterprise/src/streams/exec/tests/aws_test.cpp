#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <iostream>
#include <memory>

#include "mongo/unittest/assert.h"
#include "mongo/unittest/framework.h"

using namespace Aws;

TEST(AWS, AWSS3Test) {
    SDKOptions options;
    options.loggingOptions.logLevel = Utils::Logging::LogLevel::Error;
    InitAPI(options);

    Aws::Client::ClientConfiguration clientConfig;
    // Optional: Set to the AWS Region (overrides config file).
    clientConfig.region = "us-east-1";

    auto emptyCreds = Auth::AWSCredentials();

    Aws::S3::S3Client client(emptyCreds, nullptr, clientConfig);
    S3::Model::GetObjectRequest request;
    request.SetBucket("nathan-frank-public-test-bucket");
    request.SetKey("small_doc_test_1k.json");

    ShutdownAPI(options);
}
