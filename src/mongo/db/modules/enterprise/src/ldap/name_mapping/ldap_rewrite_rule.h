/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "rewrite_rule.h"

#include "../ldap_query_config.h"

namespace mongo {
class OperationContext;
template <typename T>
class StatusWith;

/**
 * Rewrite rule backed by LDAP queries.
 *
 * This rewrite rule will use LDAP to transform a string by using regular expressions to rewrite
 * the input into an LDAP query, and then performing it. If the query has exactly one result, it
 * is returned as the result.
 */
class LDAPRewriteRule : public RewriteRule {
    MONGO_DISALLOW_COPYING(LDAPRewriteRule);

public:
    LDAPRewriteRule(LDAPRewriteRule&& rr);
    LDAPRewriteRule& operator=(LDAPRewriteRule&& rr);

    /**
     * Factory function which returns an LDAPRewriteRule, or an error
     *
     * @param runner LDAP query runner which will be used to perform queries
     * @param match Regular expression which input will be matched against
     * @substitution Relative LDAP query URI which matched capture groups will be substituted into
     */
    static StatusWith<LDAPRewriteRule> create(const std::string& match,
                                              const std::string& substitution);

    /**
     * Rewrite 'input' into an LDAP query, and perform it, returning the result if existing
     * and singular.
     */
    StatusWith<std::string> resolve(OperationContext* txn, StringData input) const final;

    const StringData toStringData() const final;

private:
    LDAPRewriteRule(pcrecpp::RE match,
                    ComponentSubstitutionLDAPQueryConfig queryParameters,
                    std::string stringRepresentation);


    pcrecpp::RE _match;
    ComponentSubstitutionLDAPQueryConfig _queryConfig;
    std::string _stringRepresentation;  // Stored for toStringData
};
}  // namespace mongo
