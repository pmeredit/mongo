/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <memory>

#include "mongo/base/secure_allocator.h"
#include "mongo/util/duration.h"

#include "ldap_type_aliases.h"

namespace mongo {
class InternalToLDAPUserNameMapper;
class RoleName;
class ServiceContext;
class Status;
template <typename T>
class StatusWith;
class UserName;
class UserNameSubstitutionLDAPQueryConfig;

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
     * For a given user, acquires its roles from LDAP.
     */
    virtual StatusWith<std::vector<RoleName>> getUserRoles(const UserName& userName) = 0;

    /** Verifies credentials by attempting to bind to the remote LDAP server.
     *
     *  @param user, authentication username.
     *  @param pwd, authentication password.
     *  @return, Ok on successful authentication.
     */
    virtual Status verifyLDAPCredentials(const std::string& user, const SecureString& pwd) = 0;

    ////////////////////////////////////////////////////////////
    //
    // State inspection and manipulation methods.
    //
    ////////////////////////////////////////////////////////////

    /**
     * Gets/sets the list of LDAP servers to connect to.
     */
    virtual std::vector<std::string> getHosts() const = 0;
    virtual void setHosts(std::vector<std::string> hostURIs) = 0;

    /**
     * Gets/sets the LDAP server connection timeout.
     */
    virtual Milliseconds getTimeout() const = 0;
    virtual void setTimeout(Milliseconds timeout) = 0;

    /**
     * Gets/sets the LDAP server bind DN.
     */
    virtual std::string getBindDN() const = 0;
    virtual void setBindDN(const std::string& bindDN) = 0;

    /**
     * Sets the LDAP server bind password.
     */
    virtual void setBindPassword(SecureString pwd) = 0;

    /**
     * Gets/sets the LDAP user to DN mapping.
     */
    virtual std::string getUserToDNMapping() const = 0;
    virtual void setUserNameMapper(InternalToLDAPUserNameMapper nameMapper) = 0;

    /**
     * Gets/sets the LDAP authorization query template.
     */
    virtual std::string getQueryTemplate() const = 0;
    virtual void setQueryConfig(UserNameSubstitutionLDAPQueryConfig queryConfig) = 0;
};
}  // namespace mongo
