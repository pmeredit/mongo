/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "ldap_type_aliases.h"

#include <cstdint>
#include <ostream>
#include <string>

namespace mongo {

template <typename T>
class StatusWith;
class StringData;

/**
 * LDAP URIs may specify a 'scope' for a search, which defines which objects it shall cover.
 * LDAPQueryScope defines constants which map to the different acceptable scope types.
 *
 * kBase: "base object search" The search will examine the specified BaseDN, and nothing else.
 * KOne: "one-level search" The search will examine everything exactly one level below the BaseDN.
 * kSub: "subtree search" The search will examine the BaseDN object and everything below it.
 */
enum class LDAPQueryScope : std::uint8_t { kBase, kOne, kSubtree };

/**
 * Parse string representation of scope into LDAPQueryScope enum
 */
StatusWith<LDAPQueryScope> getLDAPQueryScope(StringData scope);

/**
 * Convert LDAPQueryScope enum into string representation
 */
const StringData LDAPQueryScopeToString(LDAPQueryScope scope);

/**
 * Convert LDAPQueryScope enum into ostream for unittests
 */
std::ostream& operator<<(std::ostream& os, LDAPQueryScope scope);

class UserNameSubstitutionLDAPQueryConfig;
class ComponentSubstitutionLDAPQueryConfig;

/**
 * Contains the fields contained in a RFC4516 encoded string, potentially with tokens which must
 * be substituted out, to be instantiated by an LDAPQuery.
 *
 * A generic query to find a user and its authorization roles will have some fields which are
 * known at server startup, like which attributes should be used, and some which are not fully
 * defined, like a filter which searches for an attribute which is equal to the username. An
 * LDAPQueryConfig will store both of these, but the filter string must contain a token which may
 * be searched for and replaced, in this case '{USER}'. When it is instantiated into an LDAPQuery,
 * the username must be provided, so that the substitution can occur.
 */
class LDAPQueryConfig {
public:
    /**
     * Several factory functions are provided. They are intended to be applied against different
     * formats of query strings, which specify tokens which must be substituted. When an LDAPQuery
     * is created from an LDAPQueryConfig, an instantiation function must be called, which will
     * perform this substitution. Each factory function below validates that invalid token formats
     * are absent from the query string, and produces a specialized type, which may only be used
     * with the correct substitution function.
     */

    /**
     * Constructs a set of QueryParameters from an RFC4516 encoded string.
     * This string is used directly, with no preprocessing.
     */
    static StatusWith<LDAPQueryConfig> createLDAPQueryConfig(const std::string& input);

    /**
     * Constructs a set of QueryParameters from an RFC4516 encoded string.
     * This query may contain tokens of the form '{USER}', which will be later substituted with the
     * username. The query may not contain curly brackets containing any other content.
     */
    static StatusWith<UserNameSubstitutionLDAPQueryConfig> createLDAPQueryConfigWithUserName(
        const std::string& input);

    /**
     * Constructs a set of QueryParameters from an RFC4516 encoded string.
     * This query may contain tokens with curly brackets wrapping a numeral, which will be later
     * substituted with a capture group taken from another string. The query may not
     * contain curly brackets containing any other content.
     */
    static StatusWith<ComponentSubstitutionLDAPQueryConfig> createLDAPQueryConfigWithComponents(
        const std::string& input);

    bool operator==(const LDAPQueryConfig& other) const {
        return std::tie(baseDN, scope, filter, attributes) ==
            std::tie(other.baseDN, other.scope, other.filter, other.attributes);
    }

    LDAPDN baseDN;
    LDAPQueryScope scope;
    std::string filter;
    LDAPAttributeKeys attributes;

protected:
    LDAPQueryConfig() = default;
    template <typename T>
    static StatusWith<T> createDerivedLDAPQueryConfig(const std::string& input);
};

class UserNameSubstitutionLDAPQueryConfig : public LDAPQueryConfig {};
class ComponentSubstitutionLDAPQueryConfig : public LDAPQueryConfig {};

}  // namespace mongo
