/**
 *    Copyright (C) 2022 MongoDB Inc.
 */

#pragma once

#include "mongo/crypto/jws_validated_token.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/database_name.h"
#include "sasl/idp_jwks_refresher.h"
#include "sasl/oidc_parameters_gen.h"

namespace mongo::auth {

class IdentityProvider {
public:
    IdentityProvider() = delete;

    /**
     * Initialize an IdentityProvider using the server parameter configuration.
     */
    IdentityProvider(SharedIDPJWKSRefresher refresher, IDPConfiguration cfg);

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
     * Convenience wrapper for commonly accessed 'audience' field.
     */
    StringData getAudience() const {
        return _config.getAudience();
    }

    /**
     * Determines whether this IdP should return a clientID in its SASL reply.
     */
    bool shouldReturnClientId() const {
        return _config.getSupportsHumanFlows();
    }

    /**
     * Extract and transform as needed to produce a MongoDB
     * principal name and set of RoleNames.
     */
    StatusWith<std::string> getPrincipalName(const crypto::JWSValidatedToken&,
                                             bool includePrefix = true) const;
    StatusWith<std::set<RoleName>> getUserRoles(const crypto::JWSValidatedToken&,
                                                const boost::optional<TenantId>&) const;

    /**
     * Determines whether or not tokens from this IDP are expected to contain user roles.
     */
    bool shouldTokenContainUserRoles() const;

    /**
     * Serializes the currently loaded configuration for the identity provider.
     */
    void serializeConfig(BSONObjBuilder*) const;

    /**
     * Get the JWKS refresher used by this IDP.
     */
    SharedIDPJWKSRefresher getKeyRefresher() const;

private:
    IDPConfiguration _config;
    SharedIDPJWKSRefresher _keyRefresher;
};

}  // namespace mongo::auth
