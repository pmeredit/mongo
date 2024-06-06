/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "sasl_aws_server_conversation.h"

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/auth/authentication_metrics.h"
#include "mongo/db/auth/sasl_mechanism_policies.h"
#include "mongo/db/auth/sasl_mechanism_registry.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/db/auth/user.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/connection_health_metrics_parameter_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/util/net/http_client.h"
#include "mongo/util/text.h"

#include "sasl/sasl_aws_server_protocol.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo {
namespace {
constexpr auto kAwsId = "awsId"_sd;
constexpr auto kAwsArn = "awsArn"_sd;

// Retry on 5xx response code or 0.
HttpClient::HttpReply doAWSSTSRequestWithRetries(const std::unique_ptr<HttpClient>& client,
                                                 StringData clientSecond,
                                                 const std::vector<char>& serverNonce,
                                                 char cbFlag,
                                                 std::string* awsAccountId) {
    auto retries = awsIam::saslAWSGlobalParams.awsSTSRetryCount;

    if (gEnableDetailedConnectionHealthMetricLogLines.load()) {
        ScopedCallbackTimer timer([&](Microseconds elapsed) {
            LOGV2(6788605,
                  "Auth metrics report",
                  "metric"_attr = "aws_sts_request",
                  "micros"_attr = elapsed.count());
        });
    }

    bool retry;
    do {
        retry = (retries--) > 0;

        try {
            auto [headers, requestBody] =
                awsIam::parseClientSecond(clientSecond, serverNonce, cbFlag, awsAccountId);

            ConstDataRange body(requestBody.c_str(), requestBody.size());
            client->setHeaders(headers);

            auto reply = client->request(
                HttpClient::HttpMethod::kPOST, awsIam::saslAWSGlobalParams.awsSTSUrl, body);

            if (((reply.code > 0) && (reply.code < 500)) || (reply.code >= 600)) {
                return reply;
            }

            StringData replyBody;
            reply.body.getCursor().readInto<StringData>(&replyBody);
            LOGV2_DEBUG(6205300,
                        4,
                        "Received server error from AWS STS",
                        "code"_attr = reply.code,
                        "body"_attr = replyBody,
                        "willRetry"_attr = retry);
            if (!retry) {
                return reply;
            }
        } catch (...) {
            auto status = exceptionToStatus();
            LOGV2_DEBUG(7669901,
                        4,
                        "Received HTTP error from AWS STS",
                        "__error__"_attr = status,
                        "willRetry"_attr = retry);
        }
    } while (retry);

    // Return a generic 500 reply if we have exhausted our retries due to exceptions
    return HttpClient::HttpReply(500, {}, {});
}
}  // namespace

void SaslAWSServerMechanism::appendExtraInfo(BSONObjBuilder* bob) const {
    if (!_awsId.empty()) {
        bob->append(kAwsId, _awsId);
    }

    if (!_awsFullArn.empty()) {
        bob->append(kAwsArn, _awsFullArn);
    }
}

StatusWith<std::tuple<bool, std::string>> SaslAWSServerMechanism::stepImpl(OperationContext* opCtx,
                                                                           StringData inputData) {

    if (_step > _maxStep) {
        return Status(ErrorCodes::AuthenticationFailed,
                      str::stream() << "Invalid AWS authentication step: " << _step);
    }

    _step++;

    try {
        if (_step == 1) {
            return _firstStep(opCtx, inputData);
        }

        return _secondStep(opCtx, inputData);
    } catch (...) {
        return exceptionToStatus();
    }
}

StatusWith<std::tuple<bool, std::string>> SaslAWSServerMechanism::_firstStep(
    OperationContext* opCtx, StringData inputData) {

    std::string outputData = awsIam::generateServerFirst(inputData, &_serverNonce, &_cbFlag);

    return std::make_tuple(false, std::move(outputData));
}

StatusWith<std::tuple<bool, std::string>> SaslAWSServerMechanism::_secondStep(
    OperationContext* opCtx, StringData inputData) {

    std::unique_ptr<HttpClient> request;

    if (awsIam::saslAWSGlobalParams.awsSTSUseConnectionPool.load()) {
        request = HttpClient::create();
    } else {
        request = HttpClient::createWithoutConnectionPool();
    }

    if (getTestCommandsEnabled()) {
        request->allowInsecureHTTP(true);
    }

    // Set the principal name to the AWS Account ID so that if sts::getCallerIdentity fails,
    // we give the user a hint to which account failed.
    auto result = doAWSSTSRequestWithRetries(request, inputData, _serverNonce, _cbFlag, &_awsId);
    auto cdrcBody = result.body.getCursor();
    StringData httpBody;
    cdrcBody.readInto<StringData>(&httpBody);

    if (result.code != 200) {
        auto cdrcHeader = result.header.getCursor();
        StringData httpHeader;
        cdrcHeader.readInto<StringData>(&httpHeader);

        LOGV2_WARNING(4690900,
                      "Failed connecting to AWS STS",
                      "awsSTSURL"_attr = awsIam::saslAWSGlobalParams.awsSTSUrl,
                      "HTTPReply"_attr = BSON("code" << result.code << "header" << httpHeader
                                                     << "body" << httpBody));

        uasserted(ErrorCodes::OperationFailed,
                  str::stream() << "Failed connecting to AWS STS. HTTP Status Code: "
                                << result.code);
    }

    // Set the principal name to the ARN from AWS.
    _awsFullArn = awsIam::getArn(httpBody);
    ServerMechanismBase::_principalName = awsIam::makeSimplifiedArn(_awsFullArn);

    return std::make_tuple(true, std::string());
}


namespace {

StatusWith<std::string> getHostFromURL(StringData str) {
    // Remove http:// or https:// prefix, trim port
    std::string host;
    if (str.startsWith("http://")) {
        host = str.substr(7).toString();
    } else if (str.startsWith("https://")) {
        host = str.substr(8).toString();
    } else {
        MONGO_UNREACHABLE;
    }

    size_t colon = host.find(':');
    if (colon != std::string::npos) {
        return host.substr(0, colon);
    }

    size_t trailingSlash = host.find('/');
    if (trailingSlash != std::string::npos) {
        return host.substr(0, trailingSlash);
    }

    return host;
}

MONGO_INITIALIZER_WITH_PREREQUISITES(InitializeAWSServer, ("EndStartupOptionStorage"))
(InitializerContext* context) {
    StringData str(awsIam::saslAWSGlobalParams.awsSTSUrl);
    if (!getTestCommandsEnabled()) {
        if (!str.empty() && !str.startsWith("https://")) {
            uasserted(ErrorCodes::BadValue, "STS URL must start with https://");
        }
    }

    auto swHost = getHostFromURL(str);
    uassertStatusOK(swHost);
    awsIam::saslAWSGlobalParams.awsSTSHost = swHost.getValue();
}

GlobalSASLMechanismRegisterer<AWSServerFactory> awsRegisterer;

}  // namespace
}  // namespace mongo
