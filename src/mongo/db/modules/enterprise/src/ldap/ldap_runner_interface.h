/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "ldap_type_aliases.h"

namespace mongo {
class LDAPQuery;
template <typename T>
class StatusWith;

/** An interface to abstract the execution of LDAPQueries.
 *
 * This is intended to have a concrete implementation to talk to servers, and a mocked one.
 */
class LDAPRunnerInterface {
public:
    virtual ~LDAPRunnerInterface() = default;

    /** Execute the query, returning the results.
     *
     * @param query The query to run against the remote LDAP server
     * @return Either an error arising from the operation, or the results
     */
    virtual StatusWith<LDAPEntityCollection> runQuery(const LDAPQuery& query) = 0;
};
}  // namespace mongo
