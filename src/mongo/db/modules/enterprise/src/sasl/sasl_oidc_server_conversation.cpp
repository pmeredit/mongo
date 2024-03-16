/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "sasl/idp_manager.h"
#include "sasl/oidc_parameters_gen.h"
#include "sasl/sasl_oidc_server_conversation.h"

#include "mongo/base/data_type_validated.h"
#include "mongo/crypto/jwt_types_gen.h"
#include "mongo/db/auth/oidc_protocol_gen.h"
#include "mongo/db/client.h"
#include "mongo/db/server_feature_flags_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/rpc/object_check.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo::auth {

using namespace fmt::literals;

namespace {
GlobalSASLMechanismRegisterer<OIDCServerFactory> oidcRegisterer;

using StepTuple = std::tuple<bool, std::string>;

BSONObj extractExtraInfo(const IDPConfiguration& config, const crypto::JWSValidatedToken& token) {
    auto logClaims = config.getLogClaims();
    if (!logClaims) {
        // Unexpected.  IDPConfigution::set should default this if it wasn't set by user.
        return {};
    }

    auto body = token.getBodyBSON();
    BSONObjBuilder builder;
    BSONObjBuilder claims(builder.subobjStart("claims"));
    for (const auto& claim : logClaims.get()) {
        auto elem = body[claim];
        if (!elem) {
            // Claim not found.
            continue;
        }
        claims.appendAs(elem, claim);
    }
    claims.doneFast();
    return builder.obj();
}

}  // namespace

bool OIDCServerFactory::canMakeMechanismForUser(const User* user) const {
    // Never advertise this mechanism through mech negotiation while it is in preview.
    return false;
}

StatusWith<StepTuple> SaslOIDCServerMechanism::stepImpl(OperationContext* opCtx,
                                                        StringData input) try {
    uassert(ErrorCodes::MechanismUnavailable,
            str::stream() << "Unknown SASL mechanism: " << kMechanismMongoOIDC,
            IDPManager::isOIDCEnabled());

    bool apiStrict = APIParameters::get(opCtx).getAPIStrict().value_or(false);
    uassert(ErrorCodes::MechanismUnavailable,
            str::stream() << "Preview mechanism '" << kMechanismMongoOIDC
                          << "' is unavailable in Strict API",
            !apiStrict);

    ConstDataRange cdr(input.rawData(), input.size());
    auto payload = cdr.read<Validated<BSONObj>>().val;

    switch (++_step) {
        case 1:
            return _step1(opCtx, payload);
        case 2:
            return _step2(opCtx, payload);
        default:
            return Status(ErrorCodes::OperationFailed,
                          str::stream()
                              << kMechanismMongoOIDC << " has reached invalid step " << _step);
    }

    MONGO_UNREACHABLE;
} catch (const DBException& ex) {
    return ex.toStatus();
}

StepTuple SaslOIDCServerMechanism::_step1(OperationContext* opCtx, BSONObj payload) {
    // Optimistically try to parse initial payload as fully signed token if the jwt
    // field is present.
    if (payload[OIDCMechanismClientStep2::kJWTFieldName]) {
        auto ret = _step2(opCtx, payload);
        ++_step;
        return ret;
    }

    auto request = OIDCMechanismClientStep1::parse(IDLParserContext{"oidc"}, payload);
    auto hint = request.getPrincipalName();
    if (hint) {
        _principalNameHint = hint->toString();
    }

    _idp = uassertStatusOK(IDPManager::get()->selectIDP(hint));
    auto config = _idp->getConfig();

    OIDCMechanismServerStep1 reply;
    reply.setIssuer(config.getIssuer());
    reply.setRequestScopes(config.getRequestScopes());

    // Include the clientID in the reply if supportHumanFlows has been enabled.
    if (_idp->shouldReturnClientId()) {
        reply.setClientId(config.getClientId());
    }

    auto doc = reply.toBSON();
    return {false, std::string(doc.objdata(), doc.objsize())};
}

StepTuple SaslOIDCServerMechanism::_step2(OperationContext* opCtx, BSONObj payload) {
    auto request = OIDCMechanismClientStep2::parse(IDLParserContext{"oidc"}, payload);
    auto issuerAndAudience =
        uassertStatusOK(crypto::JWSValidatedToken::extractIssuerAndAudienceFromCompactSerialization(
            request.getJWT()));

    uassert(ErrorCodes::BadValue,
            "OIDC token must contain exactly one audience",
            issuerAndAudience.audience.size() == 1);
    auto& issuer = issuerAndAudience.issuer;
    auto& audience = issuerAndAudience.audience.front();

    if (_idp) {
        uassert(
            ErrorCodes::BadValue,
            "Token issuer '{}' does not match that inferred from principal name hint '{}'"_format(
                issuer, _idp->getIssuer()),
            issuer == _idp->getIssuer());
        uassert(
            ErrorCodes::BadValue,
            "Token audience '{}' does not match that inferred from principal name hint '{}'"_format(
                audience, _idp->getAudience()),
            audience == _idp->getAudience());
    } else {
        // _idp will only be present if we performed step1.
        _idp = uassertStatusOK(IDPManager::get()->getIDP(issuer, audience));
    }
    auto token = uassertStatusOK(_idp->validateCompactToken(request.getJWT()));

    auto principalName =
        uassertStatusOK(_idp->getPrincipalName(token, false /*Don't include authNamePrefix*/));

    if (_principalNameHint) {
        uassert(ErrorCodes::BadValue,
                str::stream() << "Principal name changed between step1 '{}' and step2 '{}'"_format(
                    _principalNameHint.get(), principalName),
                _principalNameHint.get() == principalName);
    }

    // principal name in UserRequest will include the authNamePrefix
    _principalName = uassertStatusOK(_idp->getPrincipalName(token));
    _mechanismData = request.getJWT().toString();
    _expirationTime = token.getBody().getExpiration();
    _extraInfo = extractExtraInfo(_idp->getConfig(), token);

    return {true, std::string{}};
}

UserRequest SaslOIDCServerMechanism::getUserRequest() const {
    auto request = ServerMechanismBase::getUserRequest();
    request.mechanismData = _mechanismData;
    return request;
}

}  // namespace mongo::auth
