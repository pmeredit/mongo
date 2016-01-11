/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#ifdef _WIN32
#include "winldap.h"
#include "winber.h"  // winldap.h must be included before
#else
#include <ldap.h>
#endif

#include "ldap_connection_helpers.h"

#include "mongo/util/assert_util.h"

#include "../ldap_query_config.h"

namespace mongo {

int mapScopeToLDAP(const LDAPQueryScope& type) {
    if (type == LDAPQueryScope::kBase) {
        return LDAP_SCOPE_BASE;
    } else if (type == LDAPQueryScope::kSubtree) {
        return LDAP_SCOPE_SUBTREE;
    } else if (type == LDAPQueryScope::kOne) {
        return LDAP_SCOPE_ONELEVEL;
    }
    // We should never create an invalid scope
    MONGO_UNREACHABLE
}

}  // namespace mongo
