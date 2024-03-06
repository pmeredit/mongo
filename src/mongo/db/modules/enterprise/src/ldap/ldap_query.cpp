/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_query.h"

#include <boost/algorithm/string.hpp>
#include <iomanip>
#include <string>

#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/builder.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

namespace {

/**
 * For each replacement pair, replace all occurrences of first with second in *str.
 * If *str does not equal the replacement value then *str is updated. Returns true
 * if any replacements occurred.
 */
using ReplacementList = std::initializer_list<std::pair<StringData, StringData>>;
bool substituteTokens(std::string* str, const ReplacementList& replacements) {
    auto replacementOccurred = false;
    for (const auto& replacement : replacements) {
        auto transformed = boost::replace_all_copy(*str, replacement.first, replacement.second);
        if (transformed != *str) {
            *str = transformed;
            replacementOccurred = true;
        }
    }

    return replacementOccurred;
}
/**
 * RFC4515 escaping for RFC4514-style escaped DNs so those strings can be used
 * in an LDAP filter.
 */
std::string escapeDN(const std::string& input) {
    StringBuilder output;

    /* Escape sequence. RFC 4514-style escaped sequences like '\,' will become '\5c,'
     * '\5c' is RFC4515 representation of RFC4514 escape character inside LDAP filter.
     */
    constexpr auto escape_sequence = "\\"_sd;

    for (char c : input) {
        /* input MUST be encoded in a way as specified in the section 3 in RFC4514:
             ; The following characters are to be escaped when they appear
             ; in the value to be encoded: ESC, one of <escaped>, leading
             ; SHARP or SPACE, trailing SPACE, and NULL.
             string =   [ ( leadchar / pair ) [ *( stringchar / pair )
             ( trailchar / pair ) ] ]

             leadchar = LUTF1 / UTFMB
             LUTF1 = %x01-1F / %x21 / %x24-2A / %x2D-3A /
             %x3D / %x3F-5B / %x5D-7F

             trailchar  = TUTF1 / UTFMB
             TUTF1 = %x01-1F / %x21 / %x23-2A / %x2D-3A /
                 %x3D / %x3F-5B / %x5D-7F

             stringchar = SUTF1 / UTFMB
             SUTF1 = %x01-21 / %x23-2A / %x2D-3A /
                 %x3D / %x3F-5B / %x5D-7F

             pair = ESC ( ESC / special / hexpair )
             special = escaped / SPACE / SHARP / EQUALS
             escaped = DQUOTE / PLUS / COMMA / SEMI / LANGLE / RANGLE
             hexstring = SHARP 1*hexpair
             hexpair = HEX HEX

       Everything outside of UTF1SUBSET and UTFMB must be escaped
           The following is RFC 4515, section 3 excerpt:
             valueencoding  = 0*(normal / escaped)
             normal         = UTF1SUBSET / UTFMB
             escaped        = ESC HEX HEX
             UTF1SUBSET     = %x01-27 / %x2B-5B / %x5D-7F
             ; UTF1SUBSET excludes 0x00 (NUL), LPAREN,
             ; RPAREN, ASTERISK, and ESC.
         */
        if (c == '\0' || c == '(' || c == ')' || c == '*' || c == '\\') {
            std::stringstream stream;
            stream << escape_sequence << std::setfill('0') << std::setw(2) << std::hex
                   << static_cast<int>(c);
            output << stream.str();
        } else {
            output << c;
        }
    }
    return output.str();
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
    const UserNameSubstitutionLDAPQueryConfig& parameters,
    StringData userName,
    StringData originalUserName) {
    LDAPQuery instance(parameters);

    std::string escapedDN = escapeDN(userName.toString());

    bool replacedDN = substituteTokens(&instance._baseDN, {{kUserNameMatchToken, userName}});
    bool replacedFilter = substituteTokens(
        &instance._filter,
        {{kUserNameMatchToken, escapedDN}, {kProvidedUserNameMatchToken, originalUserName}});
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
        ReplacementList replacements = {{token, components[i]}};

        bool replacedDN = substituteTokens(&instance._baseDN, replacements);
        bool replacedFilter = substituteTokens(&instance._filter, replacements);

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
