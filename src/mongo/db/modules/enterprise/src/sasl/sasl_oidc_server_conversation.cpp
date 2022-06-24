/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "sasl/oidc_params.h"
#include "sasl/oidc_types_gen.h"
#include "sasl/sasl_oidc_server_conversation.h"

#include "mongo/base/data_type_validated.h"
#include "mongo/db/auth/oidc_authentication_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/rpc/object_check.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo {
namespace {
GlobalSASLMechanismRegisterer<OIDCServerFactory> oidcRegisterer;

using StepTuple = std::tuple<bool, std::string>;

bool isEnabled() {
    if (!gFeatureFlagOIDCSpike.isEnabledAndIgnoreFCV()) {
        return false;
    }

    if (getTestCommandsEnabled()) {
        return false;
    }

    return !oidcGlobalParams.authURL.empty() && !oidcGlobalParams.clientId.empty() &&
        !oidcGlobalParams.clientSecret.empty();
}

}  // namespace

bool OIDCServerFactory::canMakeMechanismForUser(const User* user) const {
    return isEnabled() && user->getCredentials().isExternal;
}

StatusWith<StepTuple> SaslOIDCServerMechanism::stepImpl(OperationContext*, StringData input) try {
    uassert(ErrorCodes::MechanismUnavailable,
            str::stream() << "Unknown SASL mechanism: " << kOIDCMechanismName,
            !isEnabled());

    ConstDataRange cdr(input.rawData(), input.size());
    auto payload = cdr.read<Validated<BSONObj>>().val;

    switch (++_step) {
        case 1:
            return _step1(payload);
        case 2:
            return _step2(payload);
        default:
            return Status(ErrorCodes::OperationFailed,
                          str::stream()
                              << kOIDCMechanismName << " has reached invalid step " << _step);
    }

    MONGO_UNREACHABLE;
} catch (const DBException& ex) {
    return ex.toStatus();
}

StepTuple SaslOIDCServerMechanism::_step1(BSONObj payload) {
    // Optimistically try to parse initial payload as fully signed token.
    try {
        auto ret = _step2(payload);
        ++_step;
        return ret;
    } catch (const DBException&) {
        // Swallow failure, it was optimistic...
    }

    auto request = OIDCMechanismClientStep1::parse({"oidc"}, payload);
    if (request.getPrincipalName()) {
        _principalName = request.getPrincipalName()->toString();
    }

    OIDCMechanismServerStep1 reply;
    reply.setIdp(oidcGlobalParams.authURL);
    reply.setClientId(oidcGlobalParams.clientId);
    reply.setClientSecret(oidcGlobalParams.clientSecret);
    auto doc = reply.toBSON();
    return {false, std::string(doc.objdata(), doc.objsize())};
}

StepTuple SaslOIDCServerMechanism::_step2(BSONObj payload) {
    auto request = OIDCMechanismClientStep2::parse({"oidc"}, payload);

    // TODO (SERVER-67664) Validate real token and extract username from it.
    auto principalName = request.getJWT();

    if (!_principalName.empty()) {
        uassert(ErrorCodes::BadValue,
                str::stream() << "Principal name changed between step1 '" << _principalName
                              << "' and step2 '" << principalName << "'",
                _principalName == principalName);
    } else {
        _principalName = principalName.toString();
    }

    return {true, std::string{}};
}

}  // namespace mongo
