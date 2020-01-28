/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "sasl_aws_server_conversation.h"

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/auth/sasl_mechanism_policies.h"
#include "mongo/db/auth/sasl_mechanism_registry.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/db/auth/user.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/util/net/http_client.h"
#include "mongo/util/text.h"

#include "sasl/sasl_aws_server_protocol.h"

namespace mongo {

StatusWith<std::tuple<bool, std::string>> SaslAWSServerMechanism::stepImpl(OperationContext* opCtx,
                                                                           StringData inputData) {
    if (_step > 2) {
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

    // Set the principal name to the AWS Account ID so that if sts::getCallerIdentity fails,
    // we give the user a hint to which account failed.
    auto [headers, requestBody] =
        awsIam::parseClientSecond(inputData, _serverNonce, _cbFlag, &_principalName);

    std::unique_ptr<HttpClient> request = HttpClient::create();

    ConstDataRange body(requestBody.c_str(), requestBody.size());
    request->setHeaders(headers);

    if (getTestCommandsEnabled()) {
        request->allowInsecureHTTP(true);
    }

    DataBuilder result = request->post(awsIam::saslAWSGlobalParams.awsSTSUrl, body);

    ConstDataRange cdr = result.getCursor();
    StringData output;
    cdr.readInto<StringData>(&output);

    // Set the principal name to the ARN from AWS.
    ServerMechanismBase::_principalName = awsIam::getUserId(output);

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
            return Status(ErrorCodes::BadValue, "STS URL must start with https://");
        }
    }

    auto swHost = getHostFromURL(str);
    if (!swHost.isOK()) {
        return swHost.getStatus();
    }

    awsIam::saslAWSGlobalParams.awsSTSHost = swHost.getValue();
    return Status::OK();
}

ServiceContext::ConstructorActionRegisterer ldapRegisterer{
    "AWSServerMechanismProxy", {"CreateSASLServerMechanismRegistry"}, [](ServiceContext* service) {
        auto& registry = SASLServerMechanismRegistry::get(service);
        registry.registerFactory<AWSServerFactory>();
    }};

}  // namespace
}  // namespace mongo
