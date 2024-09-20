/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/crypto/jwk_manager.h"
#include "mongo/crypto/jwks_fetcher_factory.h"
#include "mongo/platform/mutex.h"
#include "mongo/util/duration.h"
#include "sasl/oidc_parameters_gen.h"

namespace mongo::auth {

class IDPJWKSRefresher {
public:
    static constexpr Seconds kRefreshMinPeriod{10};

    /**
     * Initialize an IDPJWKSRefresher using the server parameter configuration.
     * This loads and initializes JWKs.
     */
    IDPJWKSRefresher(const JWKSFetcherFactory& factory, const IDPConfiguration& cfg);

    /**
     * When this IDP's keyset should next be refreshed.
     */
    Date_t getNextRefreshTime() const {
        if (auto secs = _pollSecs; secs.count() > 0) {
            return _lastRefresh + secs;
        } else {
            return Date_t::max();
        }
    }

    /**
     * Reload the key manager, on success returns whether an invalidation is recommended.
     */
    enum class RefreshOption {
        kIfDue,  // Typical refresh, on poll-interval.
        kNow,    // Just-in-time refresh, on unknown key.
    };
    StatusWith<bool> refreshKeys(const JWKSFetcherFactory& factory,
                                 RefreshOption option = RefreshOption::kIfDue);

    /**
     * Flushes keys and validators by creating a new instance of the keyManager.
     */
    void flushJWKManagerKeys(const JWKSFetcherFactory& factory) {
        auto newKeyManager = std::make_shared<crypto::JWKManager>(factory.makeJWKSFetcher(_issuer));
        std::atomic_exchange(&_keyManager, std::move(newKeyManager));  // NOLINT
    }

    /**
     * Serializes the JWKSet loaded in the underlying JWKManager.
     */
    void serializeJWKSet(BSONObjBuilder*) const;

    /**
     * Get the underlying key manager
     */
    std::shared_ptr<crypto::JWKManager> getKeyManager() const;

private:
    std::shared_ptr<crypto::JWKManager> _keyManager;
    stdx::mutex _refreshMutex;
    Date_t _lastRefresh;
    Seconds _pollSecs;
    std::string _issuer;
};

using SharedIDPJWKSRefresher = std::shared_ptr<IDPJWKSRefresher>;

}  // namespace mongo::auth
