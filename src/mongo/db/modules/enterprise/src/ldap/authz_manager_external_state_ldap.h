/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authz_manager_external_state.h"
#include "mongo/db/auth/authz_manager_external_state_local.h"
#include "mongo/db/auth/authz_session_external_state.h"
#include "mongo/db/auth/privilege_format.h"
#include "mongo/db/auth/user_acquisition_stats.h"
#include "mongo/util/assert_util.h"

#include "ldap_user_cache_poller.h"

namespace mongo {

/**
 * Wraps an AuthzManagerExternalStateMongod object and provided equivalent functionality, except
 * when a user description is requested for a user on the $external database.
 * When this occurs, instances of this class will acquire the user's information from an LDAP
 * server, and produce a BSON document containing roles equivalent to its LDAP groups.
 */
class AuthzManagerExternalStateLDAP : public AuthzManagerExternalState {
public:
    AuthzManagerExternalStateLDAP(
        std::unique_ptr<AuthzManagerExternalStateLocal> wrappedExternalState);

    ~AuthzManagerExternalStateLDAP() override = default;

    /**
     * Perform some LDAP specific setup, and passthrough to
     * AuthorizationManagerExternalStateMongod
     */
    Status initialize(OperationContext* opCtx) final;

    /**
     * Passthrough to AuthorizationManagerExternalStateMongod
     */
    std::unique_ptr<AuthzSessionExternalState> makeAuthzSessionExternalState(Client* client) final {
        return _wrappedExternalState->makeAuthzSessionExternalState(client);
    }


    /**
     * Passthrough to AuthorizationManagerExternalStateMongod, when userName is
     * not contained in $external, or when X509 authorization should be used. Otherwise, transform
     * the requested user name into an LDAP DN, perfom LDAP queries to acquire all LDAP groups that
     * the DN is a member of, and map these groups into MongoDB roles, recursively resolve them,
     * and return a description of the requested user possessing all necessary permissions.
     */
    Status getUserDescription(OperationContext* opCtx,
                              const UserRequest& userReq,
                              BSONObj* result,
                              const SharedUserAcquisitionStats& userAcquisitionStats) final;

    /**
     * As getUserDescription() above, but optimized for direct User object synthesis.
     */
    StatusWith<User> getUserObject(OperationContext* opCtx,
                                   const UserRequest& userReq,
                                   const SharedUserAcquisitionStats& userAcquisitionStats) final;

    /**
     * Passthrough to AuthorizationManagerExternalStateMongod
     */
    Status rolesExist(OperationContext* opCtx, const std::vector<RoleName>& roleNames) final {
        return _wrappedExternalState->rolesExist(opCtx, roleNames);
    }

    /**
     * Passthrough to AuthorizationManagerExternalStateMongod
     */
    StatusWith<ResolvedRoleData> resolveRoles(OperationContext* opCtx,
                                              const std::vector<RoleName>& roleNames,
                                              ResolveRoleOption option) final {
        return _wrappedExternalState->resolveRoles(opCtx, roleNames, option);
    }

    /**
     * Passthrough to AuthorizationManagerExternalStateMongod
     */
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

    /**
     * Passthrough to AuthorizationManagerExternalStateMongod
     */
    bool hasAnyPrivilegeDocuments(OperationContext* opCtx) final {
        return _wrappedExternalState->hasAnyPrivilegeDocuments(opCtx);
    }

    /**
     * Passthrough to AuthorizationManagerExternalStateMongod
     */
    void logOp(OperationContext* opCtx,
               AuthorizationManagerImpl* authzManager,
               StringData op,
               const NamespaceString& ns,
               const BSONObj& o,
               const BSONObj* o2) final {
        _wrappedExternalState->logOp(opCtx, authzManager, op, ns, o, o2);
    }

private:
    /**
     * Set to 0 if the invalidator has not been started, 1 if it has been started
     */
    AtomicWord<unsigned> _hasInitializedInvalidation;

    /**
     * Long running job to periodically refresh or invalidate all LDAP authorized users on $external
     */
    LDAPUserCachePoller _poller;

    /**
     * Wrapped AuthzManagerExternalState object
     */
    std::unique_ptr<AuthzManagerExternalStateLocal> _wrappedExternalState;
};

}  // namespace mongo
