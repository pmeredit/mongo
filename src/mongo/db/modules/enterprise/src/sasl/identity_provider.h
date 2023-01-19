/**
 *    Copyright (C) 2022 MongoDB Inc.
 */

#pragma once

#include <memory>
#include <vector>

#include "mongo/crypto/jwk_manager.h"
#include "mongo/crypto/jws_validated_token.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/database_name.h"
#include "mongo/platform/mutex.h"
#include "mongo/util/duration.h"
#include "sasl/oidc_parameters_gen.h"

namespace mongo::auth {

class IdentityProvider {
public:
    static constexpr Seconds kRefreshMinPeriod{10};

    IdentityProvider() = delete;

    /**
     * Initialize an IdentityProvider using the server parameter configuration.
     * Also loads and initializes JWKs.
     */
    explicit IdentityProvider(IDPConfiguration cfg);

    /**
     * When this IDP's keyset should next be refreshed.
     */
    Date_t getNextRefreshTime() const {
        if (auto secs = _config.getJWKSPollSecs(); secs.count() > 0) {
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
    StatusWith<bool> refreshKeys(RefreshOption option = RefreshOption::kIfDue);

    /**
     * Perform signature validation and return validated token.
     */
    StatusWith<crypto::JWSValidatedToken> validateCompactToken(StringData signedToken);

    /**
     * Get detailed settings for this IDP.
     */
    const IDPConfiguration& getConfig() const {
        return _config;
    }

    /**
     * Convenience wrapper for commonly accessed 'issuer' field.
     */
    StringData getIssuer() const {
        return _config.getIssuer();
    }

    /**
     * Extract and transform as needed to produce a MongoDB
     * principal name and set of RoleNames.
     */
    StatusWith<std::string> getPrincipalName(const crypto::JWSValidatedToken&) const;
    StatusWith<std::set<RoleName>> getUserRoles(const crypto::JWSValidatedToken&,
                                                const boost::optional<TenantId>&) const;

    /**
     * Serializes the currently loaded configuration for the identity provider.
     */
    void serializeConfig(BSONObjBuilder*) const;

    /**
     * Serializes the JWKSet loaded in the underlying JWKManager.
     */
    void serializeJWKSet(BSONObjBuilder*) const;

private:
    IDPConfiguration _config;
    std::shared_ptr<crypto::JWKManager> _keyManager;
    Mutex _refreshMutex = MONGO_MAKE_LATCH("IdentityProvider Refresh Mutex");
    Date_t _lastRefresh;
};

}  // namespace mongo::auth
