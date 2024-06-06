/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

#include "ldap_query_config.h"
#include "ldap_type_aliases.h"

#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"

namespace mongo {

enum class LDAPQueryContext {
    kLivenessCheck,
    kUserToDNMapping,
    kQueryTemplate,
    kUnitTest,  // Never use this outside of a unit test.
};

/**
 * Contains all the fields contained in a RFC4516 encoded string.
 * This is instantiated from an LDAPQueryConfig object.
 */
class LDAPQuery {
public:
    static StatusWith<LDAPQuery> instantiateQuery(const LDAPQueryConfig& parameters,
                                                  LDAPQueryContext ctx);

    static StatusWith<LDAPQuery> instantiateQuery(
        const UserNameSubstitutionLDAPQueryConfig& parameters,
        StringData userName,
        StringData unmappedUserName,
        LDAPQueryContext ctx);

    static StatusWith<LDAPQuery> instantiateQuery(
        const ComponentSubstitutionLDAPQueryConfig& parameters,
        const std::vector<std::string>& components,
        LDAPQueryContext ctx);

    bool operator==(const LDAPQuery& other) const {
        return std::tie(getBaseDN(), getScope(), getFilter(), getAttributes()) ==
            std::tie(other.getBaseDN(), other.getScope(), other.getFilter(), other.getAttributes());
    }

    std::string toString() const;
    BSONObj toBSON() const;

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

    /* If only "dn" is specified as an attribute,
     * then assume we're still using the object name.
     */
    bool isAcquiringAttributes() const {
        const auto& attrs = getAttributes();
        return (attrs.size() > 1) || ((attrs.size() == 1) && (attrs[0] != kLDAPDNAttribute));
    }

protected:
    LDAPQuery(const LDAPQueryConfig& templatedQuery, LDAPQueryContext ctx)
        : _baseDN(templatedQuery.baseDN),
          _filter(templatedQuery.filter),
          _templatedQuery(templatedQuery),
          _context(ctx) {}

    LDAPDN _baseDN;
    std::string _filter;
    LDAPQueryConfig _templatedQuery;
    LDAPQueryContext _context;
};

std::ostream& operator<<(std::ostream& os, const LDAPQuery& query);
}  // namespace mongo
