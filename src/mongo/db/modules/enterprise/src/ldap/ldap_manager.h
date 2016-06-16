/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <memory>

#include "mongo/base/secure_allocator.h"

#include "ldap_type_aliases.h"

namespace mongo {
class RoleName;
class ServiceContext;
class Status;
template <typename T>
class StatusWith;
class UserName;

/** An interface to abstract the execution of queries to the LDAP server.
 *
 * This is intended to have a concrete implementation to talk to servers, and a mocked one.
 */
class LDAPManager {
public:
    virtual ~LDAPManager() = default;

    static void set(ServiceContext* service, std::unique_ptr<LDAPManager> manager);

    static LDAPManager* get(ServiceContext* service);

    /**
     * For a given user, acquire its roles from LDAP
     */
    virtual StatusWith<std::vector<RoleName>> getUserRoles(const UserName& userName) = 0;

    /** Verify credentials by attempting to bind to the remote LDAP server.
     *
     *  @param user, authentication username.
     *  @param pwd, authentication password.
     *  @return, Ok on successful authentication.
     */
    virtual Status verifyLDAPCredentials(const std::string& user, const SecureString& pwd) = 0;
};
}  // namespace mongo
