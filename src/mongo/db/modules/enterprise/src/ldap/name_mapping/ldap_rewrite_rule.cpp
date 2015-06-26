/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_rewrite_rule.h"

#include <string>
#include <memory>

#include "mongo/base/status_with.h"
#include "mongo/util/assert_util.h"

#include "../ldap_query.h"
#include "../ldap_runner_interface.h"

namespace mongo {

StatusWith<LDAPRewriteRule> LDAPRewriteRule::create(LDAPRunnerInterface* runner,
                                                    const std::string& strMatch,
                                                    const std::string& strQuery) {
    pcrecpp::RE match(strMatch, pcrecpp::UTF8());

    auto swQueryParameters = LDAPQueryConfig::createLDAPQueryConfigWithComponents(strQuery);
    if (!swQueryParameters.isOK()) {
        return swQueryParameters.getStatus();
    }

    std::string stringRepresentation(std::string("{ match: \"") + strMatch + "\" ldapQuery: \"" +
                                     strQuery + "\" }");

    return LDAPRewriteRule{runner,
                           std::move(match),
                           std::move(swQueryParameters.getValue()),
                           std::move(stringRepresentation)};
}

#if defined(_WIN32) && _MSC_VER < 1900
LDAPRewriteRule::LDAPRewriteRule(LDAPRewriteRule&& rr)
    : _match(std::move(rr._match)),
      _runner(rr._runner),
      _queryConfig(std::move(rr._queryConfig)),
      _stringRepresentation(std::move(rr._stringRepresentation)) {}

LDAPRewriteRule& LDAPRewriteRule::operator=(LDAPRewriteRule&& rr) {
    _match = std::move(rr._match);
    _queryParameters = std::move(rr._queryParameters);
    _runner = rr._runner;
    _queryConfig = std::move(rr._queryConfig);
    _stringRepresentation(std::move(rr._stringRepresentation));
    return *this;
}
#else
LDAPRewriteRule::LDAPRewriteRule(LDAPRewriteRule&& rr) = default;
LDAPRewriteRule& LDAPRewriteRule::operator=(LDAPRewriteRule&& rr) = default;
#endif

LDAPRewriteRule::LDAPRewriteRule(LDAPRunnerInterface* runner,
                                 pcrecpp::RE match,
                                 ComponentSubstitutionLDAPQueryConfig queryParameters,
                                 std::string stringRepresentation)
    : _match(std::move(match)),
      _runner(runner),
      _queryConfig(std::move(queryParameters)),
      _stringRepresentation(std::move(stringRepresentation)) {
    invariant(runner != nullptr);
}

StatusWith<std::string> LDAPRewriteRule::resolve(StringData input) const {
    StatusWith<std::vector<std::string>> swExtractedMatches = _extractMatches(_match, input);
    if (!swExtractedMatches.isOK()) {
        return swExtractedMatches.getStatus();
    }

    StatusWith<LDAPQuery> swParamsStep =
        LDAPQuery::instantiateQuery(_queryConfig, swExtractedMatches.getValue());
    if (!swParamsStep.isOK()) {
        return swParamsStep.getStatus();
    }

    StatusWith<LDAPEntityCollection> swLDAPResults =
        _runner->runQuery(std::move(swParamsStep.getValue()));
    if (!swLDAPResults.isOK()) {
        return swLDAPResults.getStatus();
    }
    LDAPEntityCollection ldapResults(std::move(swLDAPResults.getValue()));

    if (ldapResults.empty()) {
        return Status(ErrorCodes::UserNotFound, "LDAP query returned no results");
    } else if (ldapResults.size() > 1) {
        return Status(ErrorCodes::UserDataInconsistent, "LDAP query returned multiple results");
    }

    return ldapResults.begin()->first;
}

const StringData LDAPRewriteRule::toStringData() const {
    return StringData(_stringRepresentation);
}
}  // namespace mongo
