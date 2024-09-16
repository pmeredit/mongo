/**
 *  Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "user_request_oidc.h"

#include "sasl/idp_manager.h"

#include "mongo/base/init.h"
#include "mongo/crypto/jws_validated_token.h"
#include "mongo/db/auth/user.h"
#include "mongo/logv2/log.h"
#include "mongo/util/str.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo {

namespace {
Status reauthStatus(Status status) {
    return {ErrorCodes::ReauthenticationRequired, status.reason()};
}
}  // namespace

UserRequest::UserRequestCacheKey UserRequestOIDC::generateUserRequestCacheKey() const {
    auto hashElements = getUserNameAndRolesVector(getUserName(), getRoles());
    hashElements.push_back(getJWTString().toString());
    return UserRequestCacheKey(getUserName(), hashElements);
}

StatusWith<std::unique_ptr<UserRequest>> UserRequestOIDC::makeUserRequestOIDC(
    UserName name,
    boost::optional<std::set<RoleName>> roles,
    std::string token,
    bool forReacquire) {
    auto request =
        std::make_unique<UserRequestOIDC>(std::move(name), std::move(roles), std::move(token));

    if (!forReacquire) {
        return std::unique_ptr<UserRequest>(std::move(request));
    }
    auto status = request->_tryAcquireRoles();
    if (!status.isOK()) {
        return status;
    }
    return std::unique_ptr<UserRequest>(std::move(request));
}

Status UserRequestOIDC::_tryAcquireRoles() {
    StatusWith<auth::IDPManager::SharedIdentityProvider> swIDP =
        Status(ErrorCodes::BadValue, "No identity provider found"_sd);

    auto swIssAndAud =
        crypto::JWSValidatedToken::extractIssuerAndAudienceFromCompactSerialization(getJWTString());

    if (!swIssAndAud.isOK()) {
        return Status::OK();
    }

    // Token must have one audience, as guaranteed by the validation in OIDC SASL step 2.
    invariant(swIssAndAud.getValue().audience.size() == 1);

    auto& issuer = swIssAndAud.getValue().issuer;
    auto& audience = swIssAndAud.getValue().audience.front();
    swIDP = auth::IDPManager::get()->getIDP(issuer, audience);

    if (!swIDP.isOK()) {
        // "Issuer" is no longer in the list of IdentityProviders configured.
        return reauthStatus(swIDP.getStatus());
    }
    auto idp = std::move(swIDP.getValue());

    // Revalidate token.
    auto swValidatedToken = idp->validateCompactToken(getJWTString());
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

    if (name.getUser() != principalName) {
        // Somehow we have a token for someone else.
        // It's possible the principalNameClaim has changed since last acquire.
        return {ErrorCodes::ReauthenticationRequired,
                str::stream() << "Principal name has changed from '" << name.getUser() << "' to '"
                              << principalName << "'"};
    }

    // If useAuthorizationClaim was set to false for the IdP, return the user request so that it can
    // fall-through to the wrapped external state.
    if (!idp->shouldTokenContainUserRoles()) {
        LOGV2_DEBUG(8163100,
                    3,
                    "Authorization claim is not configured for IDP, falling through to "
                    "LDAP/internal authorization",
                    "idp"_attr = idp->getIssuer());
        return Status::OK();
    }

    auto swRoles = idp->getUserRoles(validatedToken, name.tenantId());
    if (!swRoles.isOK()) {
        return reauthStatus(swRoles.getStatus());
    }

    // Append roles info to UserRequest and return the same object.
    setRoles(swRoles.getValue());

    LOGV2_DEBUG(7119504,
                5,
                "Translated OIDC user cache request",
                "user"_attr = getUserName(),
                "roles"_attr = getRoles());

    return Status::OK();
}

}  // namespace mongo
