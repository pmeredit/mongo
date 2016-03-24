/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <memory>

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

    /** Verify credentials by attempting to bind to the remote LDAP server.
     *
     *  @param bindOptions, options used to bind to the LDAP server including username and password.
     *  @return, Ok on successful authentication.
     */
    virtual Status verifyLDAPCredentials(const LDAPBindOptions& bindOptions) = 0;

    /** Execute the query, returning the results.
     *
     * @param query The query to run against the remote LDAP server
     * @return Either an error arising from the operation, or the results
     */
    virtual StatusWith<LDAPEntityCollection> runQuery(const LDAPQuery& query) = 0;
};
}  // namespace mongo
