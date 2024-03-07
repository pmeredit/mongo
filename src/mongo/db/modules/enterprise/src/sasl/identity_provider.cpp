/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "sasl/identity_provider.h"

#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo::auth {
namespace {
constexpr char kAuthNameDelimiter = '/';
void uassertValidToken(const IDPConfiguration& config, const crypto::JWT& token) {
    // JWSValidatedToken handles iat/exp validation for us.
    // This function validates OIDC specific fields.
    uassert(7070201,
            "Token issuer does not match identity provider issuer",
            token.getIssuer() == config.getIssuer());

    const auto& audience = token.getAudience();
    const auto& expectAudience = config.getAudience();
    StringData aud;
    if (holds_alternative<std::string>(audience)) {
        aud = get<std::string>(audience);
    } else {
        invariant(holds_alternative<std::vector<std::string>>(audience));
        const auto& auds = get<std::vector<std::string>>(audience);
        uassert(
            ErrorCodes::BadValue, "OIDC token must contain exactly one audience", auds.size() == 1);
        aud = auds.front();
    }
    uassert(ErrorCodes::BadValue,
            str::stream() << "OIDC token issued for invalid audience. Got: '" << aud
                          << "', expected: '" << expectAudience << "'",
            aud == expectAudience);
}
}  // namespace

IdentityProvider::IdentityProvider(SharedIDPJWKSRefresher refresher, IDPConfiguration cfg)
    : _config(std::move(cfg)), _keyRefresher(std::move(refresher)) {}

StatusWith<crypto::JWSValidatedToken> IdentityProvider::validateCompactToken(
    StringData signedToken) try {
    auto keyManager = _keyRefresher->getKeyManager();
    crypto::JWSValidatedToken token(keyManager.get(), signedToken);
    uassertValidToken(getConfig(), token.getBody());
    return token;
} catch (const DBException& ex) {
    return ex.toStatus();
}

// {authNamePrefix}/{principalClaimValue}
StatusWith<std::string> IdentityProvider::getPrincipalName(const crypto::JWSValidatedToken& token,
                                                           bool includePrefix) const try {
    auto principalClaim = _config.getPrincipalName();
    StringData principalName;
    if (principalClaim == "sub"_sd) {
        // Use already parsed subject field for common case.
        principalName = token.getBody().getSubject();
    } else {
        // Dig into BSON for anything custom.
        auto elem = token.getBodyBSON()[principalClaim];
        uassert(ErrorCodes::InvalidJWT,
                str::stream() << "Unable to find principal claim '" << principalClaim
                              << "' in token body",
                !elem.eoo());
        uassert(ErrorCodes::InvalidJWT,
                str::stream() << "Principal claim '" << principalClaim
                              << "' in token body is invalid type",
                elem.type() == String);
        principalName = elem.valueStringData();
    }

    uassert(ErrorCodes::InvalidJWT,
            str::stream() << "Invalid empty principal claim in token field '" << principalClaim
                          << "'",
            !principalName.empty());

    if (!includePrefix) {
        return principalName.toString();
    }

    return str::stream() << _config.getAuthNamePrefix() << kAuthNameDelimiter << principalName;
} catch (const DBException& ex) {
    return ex.toStatus();
}

// As a matter of policy, all OIDC authorization roles are resolved against the admin database.
constexpr auto kOIDCRoleDatabase = "admin"_sd;
StatusWith<std::set<RoleName>> IdentityProvider::getUserRoles(
    const crypto::JWSValidatedToken& token, const boost::optional<TenantId>& tenantId) const try {
    // getUserRoles() should never be called if the IdP has not been configured for OIDC
    // authorization.
    uassert(ErrorCodes::InternalError,
            "OIDC token should not be inspected for an authorization claim "
            "when configured for internal authorization",
            shouldTokenContainUserRoles());

    auto authzClaim = _config.getAuthorizationClaim();
    auto elem = token.getBodyBSON()[authzClaim.value()];
    uassert(ErrorCodes::InvalidJWT,
            str::stream() << "Claim '" << authzClaim << "' not found on OIDC token",
            !elem.eoo());
    uassert(ErrorCodes::InvalidJWT,
            str::stream() << "Authorization claim '" << authzClaim
                          << "' must be an array of strings",
            elem.type() == Array);

    auto roles = BSONArray(elem.Obj());
    std::set<RoleName> ret;
    DatabaseName roleDB = DatabaseNameUtil::deserialize(
        tenantId, kOIDCRoleDatabase, SerializationContext::stateDefault());
    std::transform(
        roles.begin(), roles.end(), std::inserter(ret, ret.begin()), [&](const auto& role) {
            uassert(ErrorCodes::InvalidJWT,
                    str::stream() << "Authorization claim '" << authzClaim
                                  << "' must be an array of strings",
                    role.type() == String);
            std::string roleName(str::stream() << _config.getAuthNamePrefix() << kAuthNameDelimiter
                                               << role.valueStringDataSafe());
            return RoleName(std::move(roleName), roleDB);
        });

    return ret;
} catch (const DBException& ex) {
    return ex.toStatus();
}

bool IdentityProvider::shouldTokenContainUserRoles() const {
    return _config.getUseAuthorizationClaim();
}

void IdentityProvider::serializeConfig(BSONObjBuilder* builder) const {
    _config.serialize(builder);
}

SharedIDPJWKSRefresher IdentityProvider::getKeyRefresher() const {
    return std::atomic_load(&_keyRefresher);
}

}  // namespace mongo::auth
