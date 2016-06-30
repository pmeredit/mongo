/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_rewrite_rule.h"

#include <memory>
#include <string>

#include "mongo/base/status_with.h"
#include "mongo/util/assert_util.h"

#include "../ldap_query.h"
#include "../ldap_runner.h"

namespace mongo {

StatusWith<LDAPRewriteRule> LDAPRewriteRule::create(const std::string& strMatch,
                                                    const std::string& strQuery) {
    pcrecpp::RE match(strMatch, pcrecpp::UTF8());

    auto swQueryParameters = LDAPQueryConfig::createLDAPQueryConfigWithComponents(strQuery);
    if (!swQueryParameters.isOK()) {
        return swQueryParameters.getStatus();
    }

    std::string stringRepresentation(std::string("{ match: \"") + strMatch + "\" ldapQuery: \"" +
                                     strQuery + "\" }");

    return LDAPRewriteRule{
        std::move(match), std::move(swQueryParameters.getValue()), std::move(stringRepresentation)};
}

LDAPRewriteRule::LDAPRewriteRule(LDAPRewriteRule&& rr) = default;
LDAPRewriteRule& LDAPRewriteRule::operator=(LDAPRewriteRule&& rr) = default;

LDAPRewriteRule::LDAPRewriteRule(pcrecpp::RE match,
                                 ComponentSubstitutionLDAPQueryConfig queryParameters,
                                 std::string stringRepresentation)
    : _match(std::move(match)),
      _queryConfig(std::move(queryParameters)),
      _stringRepresentation(std::move(stringRepresentation)) {}

StatusWith<std::string> LDAPRewriteRule::resolve(LDAPRunner* runner, StringData input) const {
    StatusWith<std::vector<std::string>> swExtractedMatches = _extractMatches(_match, input);
    if (!swExtractedMatches.isOK()) {
        return swExtractedMatches.getStatus();
    }

    StatusWith<LDAPQuery> swQuery =
        LDAPQuery::instantiateQuery(_queryConfig, swExtractedMatches.getValue());
    if (!swQuery.isOK()) {
        return swQuery.getStatus();
    }
    LDAPQuery query = std::move(swQuery.getValue());

    StatusWith<LDAPEntityCollection> swLDAPResults = runner->runQuery(query);
    if (!swLDAPResults.isOK()) {
        return swLDAPResults.getStatus();
    }
    LDAPEntityCollection ldapResults(std::move(swLDAPResults.getValue()));

    if (ldapResults.empty()) {
        return Status(ErrorCodes::UserNotFound,
                      str::stream() << "LDAP query '" << query.toString()
                                    << "' returned no results");
    } else if (ldapResults.size() > 1) {
        return Status(ErrorCodes::UserDataInconsistent,
                      str::stream() << "LDAP query '" << query.toString()
                                    << "' returned multiple results");
    }

    return ldapResults.begin()->first;
}

const StringData LDAPRewriteRule::toStringData() const {
    return StringData(_stringRepresentation);
}
}  // namespace mongo
