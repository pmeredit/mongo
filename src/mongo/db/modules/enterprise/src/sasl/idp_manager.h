/**
 *    Copyright (C) 2022 MongoDB Inc.
 */

#pragma once

#include <memory>
#include <set>
#include <vector>

#include "mongo/db/auth/role_name.h"
#include "sasl/identity_provider.h"
#include "sasl/oidc_parameters_gen.h"

namespace mongo {
class OperationContext;
class ServiceContetx;
namespace auth {

class IDPManager {
public:
    using SharedIdentityProvider = std::shared_ptr<IdentityProvider>;

    IDPManager();

    /**
     * Fetch the global IDP manager.
     */
    static IDPManager* get(ServiceContext*);

    /**
     * Reloads the global configuration state of all IDPs.
     * Called in response to a state change in the CWSP defining IDPs.
     */
    Status updateConfigurations(OperationContext*, const std::vector<IDPConfiguration>& cfgs);

    /**
     * Returns the earliest refresh time for all IDPs
     */
    Date_t getNextRefreshTime() const;

    /**
     * Refresh the keys for all IDPs or
     * only IDPs who's refresh period has been reached.
     */
    using RefreshOption = IdentityProvider::RefreshOption;
    Status refreshAllIDPs(OperationContext* opCtx, RefreshOption option = RefreshOption::kIfDue) {
        return _doRefreshIDPs(opCtx, boost::none, option);
    }

    Status refreshIDPs(OperationContext* opCtx,
                       const std::set<StringData>& issuerNames,
                       RefreshOption option = RefreshOption::kIfDue) {
        return _doRefreshIDPs(opCtx, issuerNames, option);
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

    void serialize(BSONArrayBuilder*) const;

private:
    Status _doRefreshIDPs(OperationContext*,
                          const boost::optional<std::set<StringData>>& issuerNames,
                          RefreshOption option);

    using ProviderList = std::vector<SharedIdentityProvider>;
    std::shared_ptr<ProviderList> _providers;
};

}  // namespace auth
}  // namespace mongo
