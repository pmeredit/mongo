/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "mongo/db/auth/authz_manager_external_state.h"
#include "mongo/db/auth/authz_manager_external_state_local.h"
#include "mongo/db/auth/authz_session_external_state.h"
#include "mongo/util/assert_util.h"

#include "ldap_user_cache_invalidator_job.h"

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

    virtual ~AuthzManagerExternalStateLDAP() = default;

    /**
     * Perform some LDAP specific setup, and passthrough to
     * AuthorizationManagerExternalStateMongod
     */
    Status initialize(OperationContext* txn) final;

    /**
     * Passthrough to AuthorizationManagerExternalStateMongod
     */
    std::unique_ptr<AuthzSessionExternalState> makeAuthzSessionExternalState(
        AuthorizationManager* authzManager) final {
        return _wrappedExternalState->makeAuthzSessionExternalState(authzManager);
    }

    /**
     * Passthrough to AuthorizationManagerExternalStateMongod
     */
    Status getStoredAuthorizationVersion(OperationContext* txn, int* outVersion) final {
        return _wrappedExternalState->getStoredAuthorizationVersion(txn, outVersion);
    }

    /**
     * Passthrough to AuthorizationManagerExternalStateMongod, when userName is
     * not contained in $external. Otherwise, transform the requested user name into an LDAP
     * DN, perfom LDAP queries to acquire all LDAP groups that the DN is a member of, and map
     * these groups into MongoDB roles, recursively resolve them,
     * and return a description of the requested user possessing all necessary permissions.
     */
    Status getUserDescription(OperationContext* txn,
                              const UserName& userName,
                              BSONObj* result) final;

    /**
     * Passthrough to AuthorizationManagerExternalStateMongod
     */
    Status getRoleDescription(OperationContext* txn,
                              const RoleName& roleName,
                              bool showPrivileges,
                              BSONObj* result) final {
        return _wrappedExternalState->getRoleDescription(txn, roleName, showPrivileges, result);
    }

    /**
     * Passthrough to AuthorizationManagerExternalStateMongod
     */
    Status getRoleDescriptionsForDB(OperationContext* txn,
                                    const std::string dbname,
                                    bool showPrivileges,
                                    bool showBuiltinRoles,
                                    std::vector<BSONObj>* result) final {
        return _wrappedExternalState->getRoleDescriptionsForDB(
            txn, dbname, showPrivileges, showBuiltinRoles, result);
    }

    /**
     * Passthrough to AuthorizationManagerExternalStateMongod
     */
    bool hasAnyPrivilegeDocuments(OperationContext* txn) final {
        return _wrappedExternalState->hasAnyPrivilegeDocuments(txn);
    }

    /**
     * Passthrough to AuthorizationManagerExternalStateMongod
     */
    void logOp(OperationContext* txn,
               const char* op,
               const char* ns,
               const BSONObj& o,
               const BSONObj* o2) final {
        _wrappedExternalState->logOp(txn, op, ns, o, o2);
    }

private:
    /**
     * Set to 0 if the invalidator has not been started, 1 if it has been started
     */
    AtomicUInt32 _hasInitializedInvalidation;

    /**
     * Long running job to periodically invalidate all LDAP authorized users on $external
     */
    LDAPUserCacheInvalidator _invalidator;

    /**
     * Wrapped AuthzManagerExternalState object
     */
    std::unique_ptr<AuthzManagerExternalStateLocal> _wrappedExternalState;
};

}  // namespace mongo
