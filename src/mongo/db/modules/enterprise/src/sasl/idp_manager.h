/**
 *    Copyright (C) 2022 MongoDB Inc.
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
                          bool invalidateOnFailure = false) {
        return _doRefreshIDPs(opCtx, boost::none, option, invalidateOnFailure);
    }

    Status refreshIDPs(OperationContext* opCtx,
                       const std::set<StringData>& issuerNames,
                       RefreshOption option = RefreshOption::kIfDue,
                       bool invalidateOnFailure = false) {
        return _doRefreshIDPs(opCtx, issuerNames, option, invalidateOnFailure);
    }

    /**
     * Select an appropriate IDP based on advertised principalName.
     */
    StatusWith<SharedIdentityProvider> selectIDP(
        const boost::optional<StringData>& principalNameHint = boost::none);

    /**
     * Get a specific IdentityProvider by issuerName.
     */
    StatusWith<SharedIdentityProvider> getIDP(StringData issuerName);

    /**
     * Number of IDP managers configured.
     */
    std::size_t size() const {
        return _providers->size();
    }

    /**
     * Serializes the currently loaded config for all identity providers.
     **/
    void serializeConfig(BSONArrayBuilder*) const;

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

    using ProviderList = std::vector<SharedIdentityProvider>;
    std::shared_ptr<ProviderList> _providers;
};

}  // namespace auth
}  // namespace mongo
