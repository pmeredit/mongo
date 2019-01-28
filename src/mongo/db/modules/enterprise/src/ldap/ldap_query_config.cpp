/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_query_config.h"

#include <limits>
#include <pcrecpp.h>

#include "mongo/base/status_with.h"
#include "mongo/stdx/functional.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/stringutils.h"

namespace mongo {
namespace {

/**
 * Decode a string potentially containing percent encoded characters, per RFC3986.
 */
StatusWith<std::string> percentDecodeString(std::string&& input) {
    std::string result;
    size_t lastParsedIndex = 0;
    const size_t octetStrWidth = 2;

    while (lastParsedIndex < input.size()) {
        size_t nextPercent = input.find('%', lastParsedIndex);
        result.append(input, lastParsedIndex, nextPercent - lastParsedIndex);
        if (nextPercent == std::string::npos) {
            break;
        }
        // nextPercent must point to a percent character preceding an encoded octet
        nextPercent += 1;

        char octet[3];
        char* const octetEnd = octet + octetStrWidth;
        char* endPtr = nullptr;

        strncpy(octet, input.c_str() + nextPercent, octetStrWidth);
        *octetEnd = '\0';

        auto decodedOctet = strtoul(octet, &endPtr, 16);
        if (decodedOctet > std::numeric_limits<unsigned char>::max() || endPtr != octetEnd) {
            return Status(ErrorCodes::FailedToParse,
                          "Failed to percent decode string: Failed to parse octet pair from hex");
        }
        result.push_back(static_cast<char>(decodedOctet));

        lastParsedIndex = nextPercent + octetStrWidth;
    }

    return result;
}

// clang-format off
const pcrecpp::RE ldapRelativeURIRegex(
    "(?:([^?]+)?" // [ dn
        "(?:\\?([^?]+)?" // [ "?" [attributes]
            "(?:\\?([^?]+)?" // [ "?" [scope]
                "(?:\\?([^?]+)?" // [ "?" [filter]
                    "(?:\\?(.+)?)?" // [Extensions]
                ")?"
            ")?"
        ")?"
    ")?"
    , pcrecpp::UTF8());
// clang-format on

const StringData kBase{"base"};
const StringData kOne{"one"};
const StringData kSub{"sub"};
}  // namespace

StatusWith<LDAPQueryScope> getLDAPQueryScope(StringData scope) {
    if (scope.equalCaseInsensitive(kBase)) {
        return LDAPQueryScope::kBase;
    } else if (scope.equalCaseInsensitive(kOne)) {
        return LDAPQueryScope::kOne;
    } else if (scope.equalCaseInsensitive(kSub)) {
        return LDAPQueryScope::kSubtree;
    } else {
        return Status{ErrorCodes::FailedToParse,
                      str::stream() << "Unrecognized query scope '" << scope << "'. Options are '"
                                    << kBase
                                    << "', '"
                                    << kOne
                                    << "', and '"
                                    << kSub
                                    << "'"};
    }
}

const StringData LDAPQueryScopeToString(LDAPQueryScope scope) {
    if (scope == LDAPQueryScope::kBase) {
        return kBase;
    } else if (scope == LDAPQueryScope::kOne) {
        return kOne;
    } else if (scope == LDAPQueryScope::kSubtree) {
        return kSub;
    }
    // We should never get here, because we should never make a bad query scope
    MONGO_UNREACHABLE
}

std::ostream& operator<<(std::ostream& os, LDAPQueryScope scope) {
    return os << LDAPQueryScopeToString(scope);
}

template <typename T>
StatusWith<T> LDAPQueryConfig::createDerivedLDAPQueryConfig(const std::string& input) {
    T params;
    std::string attributes, scope, extensions;
    params._queryConfigStr = input;

    // Use a regular expression to find all the interesting components in the query URI and copy
    // them into std::strings.
    if (!ldapRelativeURIRegex.FullMatch(
            input, &params.baseDN, &attributes, &scope, &params.filter, &extensions)) {
        return Status(ErrorCodes::FailedToParse, "Invalid LDAP URL");
    }

    // After being parsed by the regex, the attributes are stored as a comma separated string.
    // We have to tokenize them into a vector of strings, then run them through percent decoding
    // to remove percent encoded octets. We may then pass the vector into the LDAPQuery object.
    if (!attributes.empty()) {
        LDAPAttributeKeys attributesVector;
        splitStringDelim(std::move(attributes), &attributesVector, ',');
        // Percent decode all attributes
        for (LDAPAttributeKey& attribute : attributesVector) {
            StatusWith<std::string> swDecodedAttribute = percentDecodeString(std::move(attribute));
            if (!swDecodedAttribute.isOK()) {
                return swDecodedAttribute.getStatus();
            }
            params.attributes.emplace_back(std::move(swDecodedAttribute.getValue()));
        }
    }

    if (!scope.empty()) {
        StatusWith<LDAPQueryScope> swScope = getLDAPQueryScope(scope);
        if (!swScope.isOK()) {
            return swScope.getStatus();
        }
        params.scope = std::move(swScope.getValue());
    } else {
        params.scope = LDAPQueryScope::kBase;
    }

    // TODO: Parse extensions

    // Percent decode relevant strings
    StatusWith<std::string> swDecodedBaseDN = percentDecodeString(std::move(params.baseDN));
    if (!swDecodedBaseDN.isOK()) {
        return swDecodedBaseDN.getStatus();
    }
    params.baseDN = std::move(swDecodedBaseDN.getValue());

    StatusWith<std::string> swDecodedFilter = percentDecodeString(std::move(params.filter));
    if (!swDecodedFilter.isOK()) {
        return swDecodedFilter.getStatus();
    }
    params.filter = std::move(swDecodedFilter.getValue());

    return params;
}

StatusWith<LDAPQueryConfig> LDAPQueryConfig::createLDAPQueryConfig(const std::string& input) {
    return LDAPQueryConfig::createDerivedLDAPQueryConfig<LDAPQueryConfig>(input);
}

namespace {
/**
 * This free function parses an input string for {.+} tokens, and passes them to a validator
 * function. If the validator returns an invalid Status object, findAndValidateTokens will return
 * it. If all identified tokens are valid, then it returns Status::OK().
 *
 * This function is intended to abstract token acquisition from validation, so consumers can
 * specify what makes tokens valid.
 */
Status findAndValidateTokens(const std::string& input,
                             stdx::function<Status(StringData)> validatorFunction) {
    size_t braceStart = input.find('{');
    while (braceStart != std::string::npos) {
        size_t braceEnd = input.find('}', braceStart);
        if (braceEnd == std::string::npos) {
            return Status(ErrorCodes::FailedToParse,
                          str::stream() << "Unterminated curly brace at index: " << braceStart);
        }

        StringData tokenBody(input.c_str() + braceStart + 1, braceEnd - (braceStart + 1));
        if (tokenBody.empty()) {
            return Status(ErrorCodes::FailedToParse, "Expected token body, but only found '{}'");
        }

        Status tokenValidated = validatorFunction(tokenBody);
        if (!tokenValidated.isOK()) {
            return tokenValidated;
        }

        braceStart = input.find('{', braceStart + 1);
    }

    return Status::OK();
}

}  // namespace

StatusWith<UserNameSubstitutionLDAPQueryConfig> LDAPQueryConfig::createLDAPQueryConfigWithUserName(
    const std::string& input) {
    Status tokensValidated = findAndValidateTokens(input, [](StringData token) {
        const StringData userToken = "USER";
        if (token != userToken) {
            return Status(ErrorCodes::FailedToParse,
                          str::stream() << "Expected token '{USER}', but found '" << token << "'");
        }
        return Status::OK();
    });
    if (!tokensValidated.isOK()) {
        return tokensValidated;
    }

    return LDAPQueryConfig::createDerivedLDAPQueryConfig<UserNameSubstitutionLDAPQueryConfig>(
        input);
}

StatusWith<ComponentSubstitutionLDAPQueryConfig>
LDAPQueryConfig::createLDAPQueryConfigWithComponents(const std::string& input) {
    Status tokensValidated = findAndValidateTokens(input, [](StringData token) {
        for (const char& tokenChar : token) {
            if (!isdigit(tokenChar)) {
                return Status(ErrorCodes::FailedToParse,
                              str::stream() << "Expected numeric in token, but found '" << tokenChar
                                            << "'");
            }
        }

        return Status::OK();
    });
    if (!tokensValidated.isOK()) {
        return tokensValidated;
    }

    return LDAPQueryConfig::createDerivedLDAPQueryConfig<ComponentSubstitutionLDAPQueryConfig>(
        input);
}
}  // namespace mongo
