/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <memory>

#include "mongo/base/secure_allocator.h"
#include "mongo/db/auth/user_acquisition_stats.h"
#include "mongo/db/client.h"
#include "mongo/util/duration.h"
#include "mongo/util/tick_source_mock.h"

#include "ldap_connection_options.h"
#include "ldap_host.h"
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
    virtual Status bindAsUser(const std::string& user,
                              const SecureString& pwd,
                              TickSource* tickSource,
                              const SharedUserAcquisitionStats& userAcquisitionStats) = 0;

    /** Execute the query, returning the results.
     *
     * @param query The query to run against the remote LDAP server
     * @return Either an error arising from the operation, or the results
     */
    virtual StatusWith<LDAPEntityCollection> runQuery(
        const LDAPQuery& query,
        TickSource* tickSource,
        const SharedUserAcquisitionStats& userAcquisitionStats) = 0;

    /** Ping the remote LDAP server to make certain communication works.
     */
    virtual Status checkLiveness(TickSource* tickSource,
                                 const SharedUserAcquisitionStats& userAcquisitionStats) = 0;

    /** Check liveness bypassing the connection pool and using the supplied options.
     * Precondition: connectionOptions.usePooledConnection == false. It is unsafe
     *   to pool connections created with different options.
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

    virtual std::vector<LDAPHost> getHosts() const = 0;
    virtual void setHosts(std::vector<LDAPHost> hosts) = 0;
    virtual bool hasHosts() const = 0;

    virtual Milliseconds getTimeout() const = 0;
    virtual void setTimeout(Milliseconds timeout) = 0;

    virtual int getRetryCount() const = 0;
    virtual void setRetryCount(int retryCount) = 0;

    virtual std::string getBindDN() const = 0;
    virtual void setBindDN(const std::string& bindDN) = 0;

    virtual void setBindPasswords(std::vector<SecureString> pwd) = 0;

    virtual void setUseConnectionPool(bool which) = 0;
};
}  // namespace mongo
