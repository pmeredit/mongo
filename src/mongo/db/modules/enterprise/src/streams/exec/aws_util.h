/**
 *     Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListBucketsRequest.h>
#include <aws/s3/model/ListBucketsResult.h>

#include "mongo/base/string_data.h"
#include "streams/exec/context.h"

namespace streams {

// Streams implementation to support credential refresh for AWS Service Clients.
class AWSCredentialsProvider : public Aws::Auth::AWSCredentialsProvider {
public:
    AWSCredentialsProvider(Context* context, std::string connectionName);

    Aws::Auth::AWSCredentials GetAWSCredentials() override;

protected:
    void Reload() override;

private:
    void RefreshIfExpired();

    Aws::Auth::AWSCredentials _credentials;
    Context* _context{nullptr};
    std::string _connectionName;
};

// Base class to support mocking the AWS's lambda client.
class LambdaClient {
public:
    virtual ~LambdaClient() = default;

    virtual Aws::Lambda::Model::InvokeOutcome Invoke(
        const Aws::Lambda::Model::InvokeRequest& request) const = 0;
};

// Wrapper class for AWS's lambda client.
class AWSLambdaClient : public LambdaClient {
public:
    AWSLambdaClient(const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>& credentialsProvider,
                    const Aws::Client::ClientConfiguration& clientConfiguration);

    Aws::Lambda::Model::InvokeOutcome Invoke(
        const Aws::Lambda::Model::InvokeRequest& request) const override;

private:
    Aws::Lambda::LambdaClient _client;
};

// Base class to performing operations to a S3 client.
class S3Client {
public:
    virtual ~S3Client() = default;

    virtual Aws::S3::Model::ListBucketsOutcome ListBuckets(
        const Aws::S3::Model::ListBucketsRequest& request) = 0;

    virtual void PutObjectAsync(const Aws::S3::Model::PutObjectRequest& request,
                                const Aws::S3::PutObjectResponseReceivedHandler& cb) = 0;
};

// Wrapper class for AWS's S3 client.
class AWSS3Client : public S3Client {
public:
    AWSS3Client(std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentialsProvider,
                Aws::Client::ClientConfiguration clientConfiguration);

    Aws::S3::Model::ListBucketsOutcome ListBuckets(
        const Aws::S3::Model::ListBucketsRequest& request) override;

    void PutObjectAsync(const Aws::S3::Model::PutObjectRequest& request,
                        const Aws::S3::PutObjectResponseReceivedHandler& cb) override;

private:
    Aws::S3::S3Client _client;
};

bool isAWSRegion(mongo::StringData region);

}  // namespace streams
