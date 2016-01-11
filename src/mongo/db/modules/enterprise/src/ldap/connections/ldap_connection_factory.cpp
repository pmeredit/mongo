/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_connection_factory.h"

#include "mongo/base/status_with.h"
#include "mongo/stdx/memory.h"

#include "../ldap_connection_options.h"
#ifndef _WIN32
#include "openldap_connection.h"
#else
#include "windows_ldap_connection.h"
#endif

namespace mongo {

StatusWith<std::unique_ptr<LDAPConnection>> LDAPConnectionFactory::create(
    const LDAPConnectionOptions& options) {
#ifndef _WIN32
    std::unique_ptr<LDAPConnection> client = stdx::make_unique<OpenLDAPConnection>(options);
#else
    std::unique_ptr<LDAPConnection> client = stdx::make_unique<WindowsLDAPConnection>(options);
#endif

    Status status = client->connect();
    if (!status.isOK()) {
        return status;
    }

    return std::move(client);
}
}  // namespace mongo
