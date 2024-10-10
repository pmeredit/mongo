/**
 *    Copyright (C) 2022-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <memory>
#include <set>
#include <vector>

#include "sasl/identity_provider.h"
#include "sasl/oidc_parameters_gen.h"

#include "mongo/crypto/jwks_fetcher_factory.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/util/background.h"

namespace mongo {
class OperationContext;
namespace auth {
/**
 * Job to periodically poll the JWKS endpoints of all configured OIDC identity providers and refresh
 * the cached JWKs accordingly.
 */
class JWKSetRefreshJob : public BackgroundJob {
public:
    JWKSetRefreshJob() = default;

protected:
    std::string name() const final {
        return "JWKSetRefresher";
    }

    void run() final;
};

class IDPManager {
public:
    using SharedIdentityProvider = std::shared_ptr<IdentityProvider>;

    IDPManager(std::unique_ptr<JWKSFetcherFactory> typeFactory);

    /**
     * Fetch the global IDP manager.
     */
    static IDPManager* get();

    /**
     * Determine if the server is configured to handle OIDC as an authentication mechanism.
     */
    static bool isOIDCEnabled();

    /**
     * Reloads the global configuration state of all IDPs.
     * Called in response to a state change in the server parameter defining IDPs.
     */
    void updateConfigurations(OperationContext*, const std::vector<IDPConfiguration>& cfgs);

    /**
     * Parses an array of BSON objects into parsed objects.
     */
    static std::vector<IDPConfiguration> parseConfigFromBSONObj(BSONArray config);

    /**
     * Returns the earliest refresh time for all IDPs
     */
    Date_t getNextRefreshTime() const;

    /**
     * Refresh the keys for all IDPs or
     * only IDPs who's refresh period has been reached.
     */
    using RefreshOption = IDPJWKSRefresher::RefreshOption;
    Status refreshAllIDPs(OperationContext* opCtx,
                          RefreshOption option = RefreshOption::kIfDue,
                          bool invalidateOnFailure = false);

    Status refreshIDPs(OperationContext* opCtx,
                       const std::set<StringData>& issuerNames,
                       RefreshOption option = RefreshOption::kIfDue,
                       bool invalidateOnFailure = false);

    /**
     * Select an appropriate IDP based on advertised principalName.
     */
    StatusWith<SharedIdentityProvider> selectIDP(
        const boost::optional<StringData>& principalNameHint = boost::none);

    /**
     * Get a specific IdentityProvider by issuerName and audienceName.
     */
    StatusWith<SharedIdentityProvider> getIDP(StringData issuerName, StringData audienceName);

    /**
     * Number of IDP managers configured.
     */
    std::size_t size() const;

    /**
     * Serializes the currently loaded config for all identity providers.
     **/
    void serializeConfig(BSONArrayBuilder*) const;

    /**
     * Starts the JWKSetRefreshJob if OIDC has been enabled on this node.
     */
    void initialize();

    /**
     * Serializes the currently loaded JWKSets for the requested identity providers.
     **/
    void serializeJWKSets(BSONObjBuilder*, const boost::optional<std::set<StringData>>&) const;

private:
    Status _doRefreshIDPs(OperationContext*,
                          const boost::optional<std::set<StringData>>& issuerNames,
                          RefreshOption option,
                          bool invalidateOnFailure);

    /**
     * Flushes keys and validators from all IDPS.
     **/
    void _flushIDPSJWKS();

    std::unique_ptr<JWKSFetcherFactory> _typeFactory;

    struct IdentityProviderCatalog {
        std::vector<SharedIdentityProvider> providersByConfigOrder;
        StringMap<StringMap<SharedIdentityProvider>> providersByIssuerAndAudience;
    };
    std::shared_ptr<IdentityProviderCatalog> _providerCatalog;

    /**
     * Set to 0 if the refresher has not been started, 1 if it has been started
     */
    AtomicWord<bool> _hasInitializedKeyRefresher;

    JWKSetRefreshJob _keyRefresher;
};

}  // namespace auth
}  // namespace mongo
