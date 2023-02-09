/**
 *  Copyright (C) 2022 MongoDB Inc.
 */


#include "mongo/platform/basic.h"

#include "authz_manager_external_state_oidc.h"

#include "mongo/base/init.h"
#include "mongo/logv2/log.h"
#include "mongo/util/str.h"
#include "sasl_oidc_server_conversation.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo::auth {
namespace {
MONGO_INITIALIZER_GENERAL(AuthzManagerExternalStateOIDCShim, (), ("CreateAuthorizationManager"))
(InitializerContext*) {
    AuthzManagerExternalState::prependShim([](std::unique_ptr<AuthzManagerExternalState> mgr) {
        return std::make_unique<AuthzManagerExternalStateOIDC>(std::move(mgr));
    });
}

Status reauthStatus(Status status) {
    return {ErrorCodes::ReauthenticationRequired, status.reason()};
}

StatusWith<UserRequest> translateRequest(OperationContext* opCtx, const UserRequest& userReq) {
    const auto& userName = userReq.name;
    if ((userName.getDB() != "$external"_sd) || userReq.mechanismData.empty()) {
        // No mechanism data or not $external DB means this isn't an OIDC request.
        return userReq;
    }

    LOGV2_DEBUG(
        7119503, 5, "Translating an OIDC user cache request for user", "user"_attr = userName);

    auto swIssuer =
        crypto::JWSValidatedToken::extractIssuerFromCompactSerialization(userReq.mechanismData);
    if (!swIssuer.isOK()) {
        // If the payload won't even parse then this was probably not actually OIDC mechanism data.
        return userReq;
    }

    auto swIDP = IDPManager::get()->getIDP(swIssuer.getValue());
    if (!swIDP.isOK()) {
        // "Issuer" is no longer in the list of IdentityProviders configured.
        return reauthStatus(swIDP.getStatus());
    }
    auto idp = std::move(swIDP.getValue());

    // Revalidate token.
    auto swValidatedToken = idp->validateCompactToken(userReq.mechanismData);
    if (!swValidatedToken.isOK()) {
        // Possibly expired or key change.
        return reauthStatus(swValidatedToken.getStatus());
    }
    auto validatedToken = std::move(swValidatedToken.getValue());

    auto swPrincipalName = idp->getPrincipalName(validatedToken);
    if (!swPrincipalName.isOK()) {
        return reauthStatus(swPrincipalName.getStatus());
    }
    auto principalName = std::move(swPrincipalName.getValue());

    if (userName.getUser() != principalName) {
        // Somehow we have a token for someone else.
        // It's possible the principalNameClaim has changed since last acquire.
        return {ErrorCodes::ReauthenticationRequired,
                str::stream() << "Principal name has changed from '" << userName.getUser()
                              << "' to '" << principalName << "'"};
    }

    auto swRoles = idp->getUserRoles(validatedToken, userName.getTenant());
    if (!swRoles.isOK()) {
        return reauthStatus(swRoles.getStatus());
    }

    // Append roles info to UserRequest and return synthetic user.
    auto newRequest = userReq;
    newRequest.roles = std::move(swRoles.getValue());

    LOGV2_DEBUG(7119504,
                5,
                "Translated OIDC user cache request",
                "user"_attr = newRequest.name,
                "roles"_attr = newRequest.roles);

    return newRequest;
}
}  // namespace

Status AuthzManagerExternalStateOIDC::getUserDescription(OperationContext* opCtx,
                                                         const UserRequest& userReq,
                                                         BSONObj* result) {
    auto swRequest = translateRequest(opCtx, userReq);
    if (!swRequest.isOK()) {
        return swRequest.getStatus();
    }

    return _wrappedExternalState->getUserDescription(opCtx, swRequest.getValue(), result);
}

StatusWith<User> AuthzManagerExternalStateOIDC::getUserObject(OperationContext* opCtx,
                                                              const UserRequest& userReq) {
    auto swRequest = translateRequest(opCtx, userReq);
    if (!swRequest.isOK()) {
        return swRequest.getStatus();
    }

    return _wrappedExternalState->getUserObject(opCtx, swRequest.getValue());
}

}  // namespace mongo::auth
