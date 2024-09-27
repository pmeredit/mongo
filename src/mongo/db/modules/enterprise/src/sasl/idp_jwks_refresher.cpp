/**
 * Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "sasl/idp_jwks_refresher.h"

#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo::auth {

IDPJWKSRefresher::IDPJWKSRefresher(const JWKSFetcherFactory& factory, const IDPConfiguration& cfg) {
    _keyManager = std::make_shared<crypto::JWKManager>(factory.makeJWKSFetcher(cfg.getIssuer()));
    _lastRefresh = Date_t::now();
    _pollSecs = cfg.getJWKSPollSecs();
    _issuer = cfg.getIssuer().toString();

    // Make a best effort to load the keyManager with keys. If the configured
    // issuer's discovery endpoint or JWKS URL are unresponsive, then the keyManager will simply
    // be empty initially. Refresh attempts will be made periodically via the JWKSetRefreshJob
    // and whenever an auth attempt with a token issued by this IdP.
    auto loadKeysStatus = _keyManager->loadKeys();
    if (!loadKeysStatus.isOK()) {
        LOGV2_WARNING(7938403,
                      "Could not load keys for identity provider",
                      "issuer"_attr = _issuer,
                      "error"_attr = loadKeysStatus.reason());
    }
}


StatusWith<bool> IDPJWKSRefresher::refreshKeys(const JWKSFetcherFactory& factory,
                                               RefreshOption option) try {
    if ((option == RefreshOption::kIfDue) && (getNextRefreshTime() > Date_t::now())) {
        return false;
    }

    stdx::unique_lock<stdx::mutex> lk(_refreshMutex, stdx::try_to_lock);
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
    // We create a new shared_ptr pointing to the same underlying object as _keyManager to prevent
    // `use after free` in the case where oidcRefreshKeys is called and the underlying JWKManager
    // is deleted while we were comparing old keys with new ones.
    auto oldKeyManager = getKeyManager();
    auto newKeyManager = std::make_shared<crypto::JWKManager>(factory.makeJWKSFetcher(_issuer));

    auto keyRefreshStatus = newKeyManager->loadKeys();
    if (!keyRefreshStatus.isOK()) {
        LOGV2_DEBUG(7938404,
                    3,
                    "JWK refresh failed for identity provider",
                    "issuer"_attr = _issuer,
                    "error"_attr = keyRefreshStatus.reason());
        return keyRefreshStatus;
    }

    const auto& newKeys = newKeyManager->getKeys();

    // If a key was removed from our keyManager during our process of just in time refresh we will
    // set the flag for invalidation.
    std::atomic_exchange(&_keyManager, std::move(newKeyManager));  // NOLINT
    bool invalidate = oldKeyManager->getIsKeyModified() ||
        std::any_of(oldKeyManager->getKeys().cbegin(),
                    oldKeyManager->getKeys().cend(),
                    [&](const auto& entry) {
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

void IDPJWKSRefresher::serializeJWKSet(BSONObjBuilder* builder) const {
    auto currentKeyManager = std::atomic_load(&_keyManager);  // NOLINT
    currentKeyManager->serialize(builder);
}

std::shared_ptr<crypto::JWKManager> IDPJWKSRefresher::getKeyManager() const {
    return std::atomic_load(&_keyManager);  // NOLINT;
}

}  // namespace mongo::auth
