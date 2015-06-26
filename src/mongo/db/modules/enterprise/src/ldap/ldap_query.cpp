/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_query.h"

#include "pcrecpp.h"
#include <string>

#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/builder.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

namespace {
const pcrecpp::RE kUserNameMatchToken = pcrecpp::RE("{USER}");
}  //  namespace

// TODO: Use RFC4516 encoding here
std::string LDAPQuery::toString() const {
    StringBuilder sb;
    sb << "BaseDN: \"" << getBaseDN() << "\", "
       << "Scope: \"" << LDAPQueryScopeToString(getScope()) << "\", "
       << "Filter: \"" << getFilter() << "\"";
    if (!getAttributes().empty()) {
        sb << ", Attributes: ";
        for (const auto& attribute : getAttributes()) {
            sb << "\"" << attribute << "\", ";
        }
    }
    return sb.str();
}

StatusWith<LDAPQuery> LDAPQuery::instantiateQuery(const LDAPQueryConfig& parameters) {
    return LDAPQuery(parameters);
}

StatusWith<LDAPQuery> LDAPQuery::instantiateQuery(
    const UserNameSubstitutionLDAPQueryConfig& parameters, StringData userName) {
    LDAPQuery instance(parameters);
    invariant(kUserNameMatchToken.error().empty());
    kUserNameMatchToken.GlobalReplace(userName.toString(), &instance._baseDN);
    kUserNameMatchToken.GlobalReplace(userName.toString(), &instance._filter);
    return instance;
}

StatusWith<LDAPQuery> LDAPQuery::instantiateQuery(
    const ComponentSubstitutionLDAPQueryConfig& parameters,
    const std::vector<std::string>& components) {
    LDAPQuery instance(parameters);
    for (size_t i = 0; i < components.size(); ++i) {
        std::string matchTarget = mongoutils::str::stream() << "\\{" << i << "\\}";
        pcrecpp::RE matchExp(std::move(matchTarget));
        int matchedBaseDN =
            matchExp.GlobalReplace(pcrecpp::StringPiece(components[i]), &instance._baseDN);
        if (matchedBaseDN < 0) {
            return Status(ErrorCodes::FailedToParse,
                          str::stream() << "Failed to substitute component into baseDN. Error: "
                                        << matchedBaseDN);
        }

        int matchedFilter =
            matchExp.GlobalReplace(pcrecpp::StringPiece(components[i]), &instance._filter);
        if (matchedFilter < 0) {
            return Status(ErrorCodes::FailedToParse,
                          str::stream() << "Failed to substitute component into filter. Error: "
                                        << matchedFilter);
        }

        if (matchedBaseDN == 0 && matchedFilter == 0) {
            return Status(
                ErrorCodes::FailedToParse,
                str::stream()
                    << "Failed to substitute component into filter. Every capture group must "
                    << "be consumed, token #" << i << " is missing.");
        }
    }

    return instance;
}

std::ostream& operator<<(std::ostream& os, const LDAPQuery& query) {
    os << "{" << query.toString() << "}";
    return os;
}
}  // namespace mongo
