/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <memory>

#include "mongo/base/secure_allocator.h"
#include "mongo/db/auth/user_acquisition_stats.h"
#include "mongo/util/duration.h"

#include "ldap_connection_options.h"
#include "ldap_host.h"
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
     * Examine LDAP configuration to determine if Cyrus should be used
     * instead of native implementation for SASL authentication.
     */
    bool useCyrusForAuthN() const;

    /**
     * For a given user, acquires its roles from LDAP.
     */
    virtual StatusWith<std::vector<RoleName>> getUserRoles(
        const UserName& userName,
        TickSource* tickSource,
        const SharedUserAcquisitionStats& userAcquisitionStats) = 0;

    /** Verifies credentials by attempting to bind to the remote LDAP server.
     *
     *  @param user, authentication username.
     *  @param pwd, authentication password.
     *  @return, Ok on successful authentication.
     */
    virtual Status verifyLDAPCredentials(
        const std::string& user,
        const SecureString& pwd,
        TickSource* tickSource,
        const SharedUserAcquisitionStats& userAcquisitionStats) = 0;

    /** Ping the remote LDAP server to make certain communication works.
     * This method will bypass the connection pool and use the provided connection
     * options.
     * @see LDAPRunner::checkLiveness()
     */
    virtual Status checkLivenessNotPooled(
        const LDAPConnectionOptions& connectionOptions,
        TickSource* tickSource,
        const SharedUserAcquisitionStats& userAcquisitionStats) = 0;

    ////////////////////////////////////////////////////////////
    //
    // State inspection and manipulation methods.
    //
    ////////////////////////////////////////////////////////////

    /**
     * Gets/sets the list of LDAP servers to connect to.
     */
    virtual std::vector<LDAPHost> getHosts() const = 0;
    virtual void setHosts(std::vector<LDAPHost> hostURIs) = 0;
    virtual bool hasHosts() const = 0;

    /**
     * Gets/sets the LDAP server connection timeout.
     */
    virtual Milliseconds getTimeout() const = 0;
    virtual void setTimeout(Milliseconds timeout) = 0;

    /**
     * Gets/sets the LDAP server retry count.
     */
    virtual int getRetryCount() const = 0;
    virtual void setRetryCount(int retryCount) = 0;

    /**
     * Gets/sets the LDAP server bind DN.
     */
    virtual std::string getBindDN() const = 0;
    virtual void setBindDN(const std::string& bindDN) = 0;

    /**
     * Sets the LDAP server bind password(s).
     */
    virtual void setBindPasswords(std::vector<SecureString> pwds) = 0;

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

    /**
     * Tells whether LDAPManager has been initialized.
     */
    virtual bool isInitialized() const = 0;
};
}  // namespace mongo
