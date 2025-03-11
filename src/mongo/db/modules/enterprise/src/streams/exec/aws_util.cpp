/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/aws_util.h"

#include <aws/core/Aws.h>
#include <aws/core/Region.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/s3/model/ListBucketsResult.h>

#include "mongo/base/string_data.h"
#include "mongo/util/assert_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

AWSCredentialsProvider::AWSCredentialsProvider(Context* context, std::string connectionName)
    : Aws::Auth::AWSCredentialsProvider(), _context{context}, _connectionName{connectionName} {}

Aws::Auth::AWSCredentials AWSCredentialsProvider::GetAWSCredentials() {
    RefreshIfExpired();
    Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
    return _credentials;
}

void AWSCredentialsProvider::Reload() {
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "ConnectionName '" << _connectionName
                          << "' is no longer present in connections collection",
            _context->connections->contains(_connectionName));
    auto connection = _context->connections->at(_connectionName);
    auto connOptions = AWSIAMConnectionOptions::parse(IDLParserContext("AWSConnectionParser"),
                                                      connection.getOptions());
    _credentials = Aws::Auth::AWSCredentials(connOptions.getAccessKey().toString(),
                                             connOptions.getAccessSecret().toString(),
                                             connOptions.getSessionToken().toString(),
                                             Aws::Utils::DateTime{connOptions.getExpirationDate()});
}

void AWSCredentialsProvider::RefreshIfExpired() {
    Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
    if (!_credentials.IsExpiredOrEmpty()) {
        return;
    }

    guard.UpgradeToWriterLock();
    if (!_credentials.IsExpiredOrEmpty())  // double-checked lock to avoid refreshing twice
    {
        return;
    }

    Reload();
}

AWSLambdaClient::AWSLambdaClient(
    const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>& credentialsProvider,
    const Aws::Client::ClientConfiguration& clientConfiguration)
    : _client{credentialsProvider, clientConfiguration} {}

Aws::Lambda::Model::InvokeOutcome AWSLambdaClient::Invoke(
    const Aws::Lambda::Model::InvokeRequest& request) const {
    return _client.Invoke(request);
}

AWSS3Client::AWSS3Client(std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentialsProvider,
                         Aws::Client::ClientConfiguration clientConfiguration)
    : _client{std::move(credentialsProvider), nullptr, std::move(clientConfiguration)} {}

Aws::S3::Model::ListBucketsOutcome AWSS3Client::ListBuckets(
    const Aws::S3::Model::ListBucketsRequest& request) {
    return _client.ListBuckets(request);
}

void AWSS3Client::PutObjectAsync(const Aws::S3::Model::PutObjectRequest& request,
                                 const Aws::S3::PutObjectResponseReceivedHandler& cb) {
    _client.PutObjectAsync(request, cb);
}

bool isAWSRegion(mongo::StringData region) {
    if (region == Aws::Region::AF_SOUTH_1 || region == Aws::Region::AP_EAST_1 ||
        region == Aws::Region::AP_NORTHEAST_1 || region == Aws::Region::AP_NORTHEAST_2 ||
        region == Aws::Region::AP_NORTHEAST_3 || region == Aws::Region::AP_SOUTH_1 ||
        region == Aws::Region::AP_SOUTH_2 || region == Aws::Region::AP_SOUTHEAST_1 ||
        region == Aws::Region::AP_SOUTHEAST_2 || region == Aws::Region::AP_SOUTHEAST_3 ||
        region == Aws::Region::AP_SOUTHEAST_4 || region == Aws::Region::AP_SOUTHEAST_5 ||
        region == Aws::Region::AWS_CN_GLOBAL || region == Aws::Region::AWS_GLOBAL ||
        region == Aws::Region::AWS_ISO_B_GLOBAL || region == Aws::Region::AWS_ISO_GLOBAL ||
        region == Aws::Region::AWS_US_GOV_GLOBAL || region == Aws::Region::CA_CENTRAL_1 ||
        region == Aws::Region::CA_WEST_1 || region == Aws::Region::CN_NORTH_1 ||
        region == Aws::Region::CN_NORTHWEST_1 || region == Aws::Region::EU_CENTRAL_1 ||
        region == Aws::Region::EU_CENTRAL_2 || region == Aws::Region::EU_ISOE_WEST_1 ||
        region == Aws::Region::EU_NORTH_1 || region == Aws::Region::EU_SOUTH_1 ||
        region == Aws::Region::EU_SOUTH_2 || region == Aws::Region::EU_WEST_1 ||
        region == Aws::Region::EU_WEST_2 || region == Aws::Region::EU_WEST_3 ||
        region == Aws::Region::IL_CENTRAL_1 || region == Aws::Region::ME_CENTRAL_1 ||
        region == Aws::Region::ME_SOUTH_1 || region == Aws::Region::SA_EAST_1 ||
        region == Aws::Region::US_EAST_1 || region == Aws::Region::US_EAST_2 ||
        region == Aws::Region::US_GOV_EAST_1 || region == Aws::Region::US_GOV_WEST_1 ||
        region == Aws::Region::US_ISO_EAST_1 || region == Aws::Region::US_ISO_WEST_1 ||
        region == Aws::Region::US_ISOB_EAST_1 || region == Aws::Region::US_WEST_1 ||
        region == Aws::Region::US_WEST_2) {
        return true;
    }
    return false;
}

};  // namespace streams
