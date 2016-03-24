/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

#include "ldap_query_config.h"
#include "ldap_type_aliases.h"

namespace mongo {

template <typename T>
class StatusWith;
class StringData;

class StringSubstitutionStrategy;

/**
 * Contains all the fields contained in a RFC4516 encoded string.
 * This is instantiated from an LDAPQueryConfig object.
 */
class LDAPQuery {
public:
    static StatusWith<LDAPQuery> instantiateQuery(const LDAPQueryConfig& parameters);

    static StatusWith<LDAPQuery> instantiateQuery(
        const UserNameSubstitutionLDAPQueryConfig& parameters, StringData userName);

    static StatusWith<LDAPQuery> instantiateQuery(
        const ComponentSubstitutionLDAPQueryConfig& parameters,
        const std::vector<std::string>& components);

    bool operator==(const LDAPQuery& other) const {
        return std::tie(getBaseDN(), getScope(), getFilter(), getAttributes()) ==
            std::tie(other.getBaseDN(), other.getScope(), other.getFilter(), other.getAttributes());
    }

    std::string toString() const;

    const LDAPDN& getBaseDN() const {
        return _baseDN;
    }
    const LDAPQueryScope& getScope() const {
        return _templatedQuery.scope;
    }
    const std::string& getFilter() const {
        return _filter;
    }
    const LDAPAttributeKeys& getAttributes() const {
        return _templatedQuery.attributes;
    }

protected:
    LDAPQuery(const LDAPQueryConfig& templatedQuery)
        : _baseDN(templatedQuery.baseDN),
          _filter(templatedQuery.filter),
          _templatedQuery(templatedQuery) {}

    LDAPDN _baseDN;
    std::string _filter;
    const LDAPQueryConfig& _templatedQuery;
};

std::ostream& operator<<(std::ostream& os, const LDAPQuery& query);
}  // namespace mongo
