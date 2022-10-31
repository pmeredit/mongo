/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "sasl/identity_provider.h"

#include "sasl/oidc_parameters_gen.h"

namespace mongo::auth {
namespace {
std::string getAuthNamePrefix(const IDPConfiguration& config) {
    auto prefix = config.getAuthNamePrefix();
    if (!prefix || prefix->empty()) {
        return std::string();
    }

    return str::stream() << *prefix << "/";
}

void uassertValidToken(const IDPConfiguration& config, const crypto::JWT& token) {
    // JWSValidatedToken handles iat/exp validation for us.
    // This function validates OIDC specific fields.
    uassert(7070201,
            "Token issuer does not match identity provider issuer",
            token.getIssuer() == config.getIssuer());

    const auto& audience = token.getAudience();
    const auto& expectAudience = config.getAudience();
    if (stdx::holds_alternative<std::string>(audience)) {
        StringData aud = stdx::get<std::string>(audience);
        uassert(ErrorCodes::BadValue,
                str::stream() << "OIDC token issued for invalid autience. Got: '" << aud
                              << "', expected: '" << expectAudience << "'",
                aud == expectAudience);
    } else {
        invariant(stdx::holds_alternative<std::vector<std::string>>(audience));
        auto auds = stdx::get<std::vector<std::string>>(audience);
        uassert(ErrorCodes::BadValue,
                str::stream()
                    << "None of the audiences issued for this OIDC token match the expected value '"
                    << expectAudience << "'",
                std::any_of(auds.cbegin(), auds.cend(), [&](const auto& aud) {
                    return aud == expectAudience;
                }));
    }
}
}  // namespace

IdentityProvider::IdentityProvider(IDPConfiguration cfg)
    : _config(std::move(cfg)),
      _keyManager(std::make_shared<crypto::JWKManager>(_config.getJWKS())),
      _lastRefresh(Date_t::now()) {}

StatusWith<crypto::JWSValidatedToken> IdentityProvider::validateCompactToken(
    StringData signedToken) try {
    crypto::JWSValidatedToken token(*_keyManager, signedToken);
    uassertValidToken(getConfig(), token.getBody());
    return token;
} catch (const DBException& ex) {
    return ex.toStatus();
}

StatusWith<bool> IdentityProvider::refreshKeys(RefreshOption option) try {
    if ((option == RefreshOption::kIfDue) && (getNextRefreshTime() > Date_t::now())) {
        return Status::OK();
    }

    stdx::unique_lock<Mutex> lk(_refreshMutex, stdx::try_to_lock);
    if (!lk.owns_lock()) {
        // A refresh is currently in progress in another thread.
        // Block on that thread to let the refresh complete, then return success
        // indicating that no invalidation is needed because the other thread
        // also would have handled that for us.
        lk.lock();
        return false;
    }

    auto currentKeyIds = _keyManager->getKeyIds();
    auto newKeyManager = std::make_shared<crypto::JWKManager>(_config.getJWKS());
    auto newKeyIds = newKeyManager->getKeyIds();

    const bool invalidate =
        std::any_of(currentKeyIds.cbegin(), currentKeyIds.cend(), [&](const auto& keyId) {
            auto swNewKey = newKeyManager->getKey(keyId);
            if (swNewKey.getStatus().code() == ErrorCodes::NoSuchKey) {
                // Key no longer exists in this JWKS.
                return true;
            }

            // If the original key material has changed, then go ahead and invalidate.
            auto oldKey = uassertStatusOK(_keyManager->getKey(keyId));
            return oldKey.woCompare(swNewKey.getValue()) != 0;
        });


    std::atomic_exchange(&_keyManager, std::move(newKeyManager));  // NOLINT
    _lastRefresh = Date_t::now();

    return invalidate;
} catch (const DBException& ex) {
    return ex.toStatus();
}

// {authNamePrefix}/{principalClaimValue}
StatusWith<std::string> IdentityProvider::getPrincipalName(
    const crypto::JWSValidatedToken& token) const try {
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

    return str::stream() << getAuthNamePrefix(_config) << principalName;
} catch (const DBException& ex) {
    return ex.toStatus();
}

// As a matter of policy, all OIDC authorization roles are resolved against the admin database.
constexpr auto kOIDCRoleDatabase = "admin"_sd;
StatusWith<std::set<RoleName>> IdentityProvider::getUserRoles(
    const crypto::JWSValidatedToken& token, const boost::optional<TenantId>& tenantId) const try {
    auto authzClaim = _config.getAuthorizationClaim();
    auto elem = token.getBodyBSON()[authzClaim];

    uassert(ErrorCodes::InvalidJWT,
            str::stream() << "Claim '" << authzClaim << "' not found on OIDC token",
            !elem.eoo());
    uassert(ErrorCodes::InvalidJWT,
            str::stream() << "Authorization claim '" << authzClaim
                          << "' must be an array of strings",
            elem.type() == Array);

    auto prefix = getAuthNamePrefix(_config);
    auto roles = BSONArray(elem.Obj());
    std::set<RoleName> ret;
    DatabaseName roleDB(kOIDCRoleDatabase, tenantId);
    std::transform(
        roles.begin(), roles.end(), std::inserter(ret, ret.begin()), [&](const auto& role) {
            uassert(ErrorCodes::InvalidJWT,
                    str::stream() << "Authorization claim '" << authzClaim
                                  << "' must be an array of strings",
                    role.type() == String);
            std::string roleName(str::stream() << prefix << role.valueStringDataSafe());
            return RoleName(std::move(roleName), roleDB);
        });

    return ret;
} catch (const DBException& ex) {
    return ex.toStatus();
}

void IdentityProvider::serialize(BSONObjBuilder* builder) const {
    _config.serialize(builder);
}

}  // namespace mongo::auth
