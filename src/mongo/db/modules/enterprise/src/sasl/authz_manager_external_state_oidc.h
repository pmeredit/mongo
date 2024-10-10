/**
 *  Copyright (C) 2022-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "sasl/idp_manager.h"

#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authz_manager_external_state.h"
#include "mongo/db/auth/authz_session_external_state.h"
#include "mongo/db/auth/user_acquisition_stats.h"

namespace mongo::auth {

class AuthzManagerExternalStateOIDC : public AuthzManagerExternalState {
public:
    AuthzManagerExternalStateOIDC() = delete;
    ~AuthzManagerExternalStateOIDC() override = default;

    explicit AuthzManagerExternalStateOIDC(std::unique_ptr<AuthzManagerExternalState> wes)
        : _wrappedExternalState(std::move(wes)), _hasInitializedKeyRefresher(0) {}

    Status initialize(OperationContext* opCtx) final {
        if (_hasInitializedKeyRefresher.swap(1) == 0 && IDPManager::isOIDCEnabled()) {
            _keyRefresher.go();
        }
        return _wrappedExternalState->initialize(opCtx);
    }

    std::unique_ptr<AuthzSessionExternalState> makeAuthzSessionExternalState(Client* client) final {
        return _wrappedExternalState->makeAuthzSessionExternalState(client);
    }

    Status getUserDescription(OperationContext* opCtx,
                              const UserRequest& userReq,
                              BSONObj* result,
                              const SharedUserAcquisitionStats& userAcquisitionStats) final;

    StatusWith<User> getUserObject(OperationContext* opCtx,
                                   const UserRequest& userReq,
                                   const SharedUserAcquisitionStats& userAcquisitionStats) final;

    Status rolesExist(OperationContext* opCtx, const std::vector<RoleName>& roleNames) final {
        return _wrappedExternalState->rolesExist(opCtx, roleNames);
    }

    StatusWith<ResolvedRoleData> resolveRoles(OperationContext* opCtx,
                                              const std::vector<RoleName>& roleNames,
                                              ResolveRoleOption option) final {
        return _wrappedExternalState->resolveRoles(opCtx, roleNames, option);
    }

    Status getRolesDescription(OperationContext* opCtx,
                               const std::vector<RoleName>& roleName,
                               PrivilegeFormat showPrivileges,
                               AuthenticationRestrictionsFormat showRestrictions,
                               std::vector<BSONObj>* result) final {
        return _wrappedExternalState->getRolesDescription(
            opCtx, roleName, showPrivileges, showRestrictions, result);
    }

    /**
     * Passthrough to AuthorizationManagerExternalStateMongod
     */
    Status getRolesAsUserFragment(OperationContext* opCtx,
                                  const std::vector<RoleName>& roleName,
                                  AuthenticationRestrictionsFormat showRestrictions,
                                  BSONObj* result) final {
        return _wrappedExternalState->getRolesAsUserFragment(
            opCtx, roleName, showRestrictions, result);
    }

    bool hasAnyPrivilegeDocuments(OperationContext* opCtx) final {
        return _wrappedExternalState->hasAnyPrivilegeDocuments(opCtx);
    }

    void logOp(OperationContext* opCtx,
               AuthorizationManagerImpl* authzManager,
               StringData op,
               const NamespaceString& ns,
               const BSONObj& o,
               const BSONObj* o2) final {
        _wrappedExternalState->logOp(opCtx, authzManager, op, ns, o, o2);
    }

private:
    std::unique_ptr<AuthzManagerExternalState> _wrappedExternalState;

    /**
     * Set to 0 if the refresher has not been started, 1 if it has been started
     */
    AtomicWord<bool> _hasInitializedKeyRefresher;

    JWKSetRefreshJob _keyRefresher;
};

}  // namespace mongo::auth
