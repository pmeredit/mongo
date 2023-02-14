/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "sasl/identity_provider.h"

#include "sasl/oidc_parameters_gen.h"

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
      _keyManager(
          std::make_shared<crypto::JWKManager>(_config.getJWKSUri(), true /* loadAtStartup */)),
      _lastRefresh(Date_t::now()) {}

StatusWith<crypto::JWSValidatedToken> IdentityProvider::validateCompactToken(
    StringData signedToken) try {
    auto keyManager = _keyManager;
    crypto::JWSValidatedToken token(keyManager.get(), signedToken);
    uassertValidToken(getConfig(), token.getBody());
    return token;
} catch (const DBException& ex) {
    return ex.toStatus();
}

StatusWith<bool> IdentityProvider::refreshKeys(RefreshOption option) try {
    if ((option == RefreshOption::kIfDue) && (getNextRefreshTime() > Date_t::now())) {
        return false;
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

    // The _keyManager may have refreshed itself during auth attempts over its lifetime, but users
    // authenticated with tokens signed by now-evicted keys would not have been invalidated. Use a
    // snapshot of the original key material to compare the current key manager's keys with the
    // newly created one.
    const auto& oldKeys = _keyManager->getKeys();
    auto newKeyManager =
        std::make_shared<crypto::JWKManager>(_config.getJWKSUri(), true /* loadAtStartup */);
    const auto& newKeys = newKeyManager->getKeys();

    // If a key was removed from our keyManager during our process of just in time refresh we will
    // set the flag for invalidation.
    auto oldKeyManager = std::atomic_exchange(&_keyManager, std::move(newKeyManager));  // NOLINT
    bool invalidate = oldKeyManager->getIsKeyModified() ||
        std::any_of(oldKeys.cbegin(), oldKeys.cend(), [&](const auto& entry) {
                          auto newKey = newKeys.find(entry.first);
                          if (newKey == newKeys.end()) {
                              // Key no longer exists in this JWKS.
                              return true;
                          }

                          // If the original key material has changed, then go ahead and invalidate.
                          return entry.second.woCompare(newKey->second) != 0;
                      });

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

    return str::stream() << _config.getAuthNamePrefix() << kAuthNameDelimiter << principalName;
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

    auto roles = BSONArray(elem.Obj());
    std::set<RoleName> ret;
    DatabaseName roleDB(kOIDCRoleDatabase, tenantId);
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

void IdentityProvider::serializeConfig(BSONObjBuilder* builder) const {
    _config.serialize(builder);
}

void IdentityProvider::serializeJWKSet(BSONObjBuilder* builder) const {
    _keyManager->serialize(builder);
}

}  // namespace mongo::auth
