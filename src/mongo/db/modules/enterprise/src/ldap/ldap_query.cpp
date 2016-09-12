/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_query.h"

#include <boost/algorithm/string.hpp>
#include <string>

#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/builder.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

namespace {
constexpr auto kUserNameMatchToken("{USER}"_sd);

/**
 * Iterate though 'input', replacing every instance of 'token' with 'replacement'
 * and writing the resulting string to 'out'. Returns true if any replacements
 * performed.
 */
bool substituteToken(const StringData input,
                     const StringData token,
                     const StringData replacement,
                     std::string* out) {
    invariant(out);
    *out = input.toString();
    boost::replace_all(*out, token, replacement);

    return *out != input;
}
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
    bool replacedDN = substituteToken(
        parameters.baseDN, kUserNameMatchToken, userName.toString(), &instance._baseDN);
    bool replacedFilter = substituteToken(
        parameters.filter, kUserNameMatchToken, userName.toString(), &instance._filter);

    if (!(replacedDN || replacedFilter)) {
        return Status(
            ErrorCodes::FailedToParse,
            str::stream()
                << "Failed to substitute component into filter. Group '{USER}' must be captured.");
    }

    return instance;
}

StatusWith<LDAPQuery> LDAPQuery::instantiateQuery(
    const ComponentSubstitutionLDAPQueryConfig& parameters,
    const std::vector<std::string>& components) {
    LDAPQuery instance(parameters);

    for (size_t i = 0; i < components.size(); ++i) {
        std::string token = mongoutils::str::stream() << "{" << i << "}";

        bool replacedDN =
            substituteToken(parameters.baseDN, token, components[i], &instance._baseDN);
        bool replacedFilter =
            substituteToken(parameters.filter, token, components[i], &instance._filter);

        if (!(replacedDN || replacedFilter)) {
            return Status(
                ErrorCodes::FailedToParse,
                str::stream()
                    << "Failed to substitute component into filter. Every capture group must "
                    << "be consumed, token #"
                    << i
                    << " is missing.");
        }
    }

    return instance;
}

std::ostream& operator<<(std::ostream& os, const LDAPQuery& query) {
    os << "{" << query.toString() << "}";
    return os;
}
}  // namespace mongo
