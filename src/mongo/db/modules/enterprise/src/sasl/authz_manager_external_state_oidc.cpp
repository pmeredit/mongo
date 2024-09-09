/**
 *  Copyright (C) 2022-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "authz_manager_external_state_oidc.h"

#include "sasl/sasl_oidc_server_conversation.h"
#include "sasl/user_request_oidc.h"

#include "authorization_manager_factory_external_impl.h"
#include "mongo/base/init.h"
#include "mongo/db/auth/authorization_manager_factory.h"
#include "mongo/db/server_feature_flags_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/util/str.h"


#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo::auth {
namespace {

Status reauthStatus(Status status) {
    return {ErrorCodes::ReauthenticationRequired, status.reason()};
}

/**
 * Resolves roles from the oidc jwt if it is provided. Returns a new request object.
 */
StatusWith<std::unique_ptr<UserRequest>> translateRequest(OperationContext* opCtx,
                                                          const UserRequest& userReq) {
    auto returnRequest = userReq.clone();
    const auto& userName = returnRequest->getUserName();

    if ((userName.getDB() != DatabaseName::kExternal.db(omitTenant)) ||
        userReq.getType() != UserRequest::UserRequestType::OIDC) {
        // No mechanism data or not $external DB means this isn't an OIDC request.
        return std::move(returnRequest);
    }

    const UserRequestOIDC* oidcRequest = reinterpret_cast<const UserRequestOIDC*>(&userReq);

    LOGV2_DEBUG(
        7119503, 5, "Translating an OIDC user cache request for user", "user"_attr = userName);

    StatusWith<IDPManager::SharedIdentityProvider> swIDP =
        Status(ErrorCodes::BadValue, "No identity provider found"_sd);

    auto swIssAndAud = crypto::JWSValidatedToken::extractIssuerAndAudienceFromCompactSerialization(
        oidcRequest->getJWTString());
    if (!swIssAndAud.isOK()) {
        // If the payload won't even parse then this was probably not actually OIDC mechanism
        // data.
        return std::move(returnRequest);
    }
    // Token must have one audience, as guaranteed by the validation in OIDC SASL step 2.
    invariant(swIssAndAud.getValue().audience.size() == 1);

    auto& issuer = swIssAndAud.getValue().issuer;
    auto& audience = swIssAndAud.getValue().audience.front();
    swIDP = IDPManager::get()->getIDP(issuer, audience);

    if (!swIDP.isOK()) {
        // "Issuer" is no longer in the list of IdentityProviders configured.
        return reauthStatus(swIDP.getStatus());
    }
    auto idp = std::move(swIDP.getValue());

    // Revalidate token.
    auto swValidatedToken = idp->validateCompactToken(oidcRequest->getJWTString());
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

    // If useAuthorizationClaim was set to false for the IdP, return the user request so that it can
    // fall-through to the wrapped external state.
    if (!idp->shouldTokenContainUserRoles()) {
        LOGV2_DEBUG(8163100,
                    3,
                    "Authorization claim is not configured for IDP, falling through to "
                    "LDAP/internal authorization",
                    "idp"_attr = idp->getIssuer());
        return std::move(returnRequest);
    }

    auto swRoles = idp->getUserRoles(validatedToken, userName.tenantId());
    if (!swRoles.isOK()) {
        return reauthStatus(swRoles.getStatus());
    }

    // Append roles info to UserRequest and return the same object.
    returnRequest->setRoles(swRoles.getValue());

    LOGV2_DEBUG(7119504,
                5,
                "Translated OIDC user cache request",
                "user"_attr = returnRequest->getUserName(),
                "roles"_attr = returnRequest->getRoles());

    return std::move(returnRequest);
}

MONGO_INITIALIZER(RegisterAuthzManagerOIDC)(InitializerContext* context) {
    createOIDCAuthzManagerExternalState =
        [](std::unique_ptr<AuthzManagerExternalState> wrappedState)
        -> std::unique_ptr<AuthzManagerExternalState> {
        return std::make_unique<AuthzManagerExternalStateOIDC>(std::move(wrappedState));
    };
};

}  // namespace

Status AuthzManagerExternalStateOIDC::getUserDescription(
    OperationContext* opCtx,
    const UserRequest& userReq,
    BSONObj* result,
    const SharedUserAcquisitionStats& userAcquisitionStats) {

    auto swRequest = translateRequest(opCtx, userReq);
    if (!swRequest.isOK()) {
        return swRequest.getStatus();
    }

    return _wrappedExternalState->getUserDescription(
        opCtx, *swRequest.getValue().get(), result, userAcquisitionStats);
}

StatusWith<User> AuthzManagerExternalStateOIDC::getUserObject(
    OperationContext* opCtx,
    const UserRequest& userReq,
    const SharedUserAcquisitionStats& userAcquisitionStats) {
    auto swRequest = translateRequest(opCtx, userReq);
    if (!swRequest.isOK()) {
        return swRequest.getStatus();
    }

    return _wrappedExternalState->getUserObject(
        opCtx, *swRequest.getValue().get(), userAcquisitionStats);
}

}  // namespace mongo::auth
