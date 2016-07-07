/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <memory>

#include "mongo/base/secure_allocator.h"
#include "mongo/util/duration.h"

#include "ldap_type_aliases.h"

namespace mongo {
struct LDAPBindOptions;
class LDAPQuery;
class ServiceContext;
class Status;
template <typename T>
class StatusWith;

/** An interface to abstract the execution of LDAPQueries.
 *
 * This is intended to have a concrete implementation to talk to servers, and a mocked one.
 */
class LDAPRunner {
public:
    virtual ~LDAPRunner() = default;

    static void set(ServiceContext* service, std::unique_ptr<LDAPRunner> runner);

    static LDAPRunner* get(ServiceContext* service);

    /** Attempt to bind to the remote LDAP server.
     *
     *  @param user, bind username.
     *  @param pwd, bind password.
     *  @return, Ok on successful authentication.
     */
    virtual Status bindAsUser(const std::string& user, const SecureString& pwd) = 0;

    /** Execute the query, returning the results.
     *
     * @param query The query to run against the remote LDAP server
     * @return Either an error arising from the operation, or the results
     */
    virtual StatusWith<LDAPEntityCollection> runQuery(const LDAPQuery& query) = 0;

    ////////////////////////////////////////////////////////////
    //
    // State inspection and manipulation methods.
    //
    ////////////////////////////////////////////////////////////

    virtual std::vector<std::string> getHosts() const = 0;
    virtual void setHosts(std::vector<std::string> hosts) = 0;

    virtual Milliseconds getTimeout() const = 0;
    virtual void setTimeout(Milliseconds timeout) = 0;

    virtual std::string getBindDN() const = 0;
    virtual void setBindDN(const std::string& bindDN) = 0;

    virtual void setBindPassword(SecureString pwd) = 0;
};
}  // namespace mongo
