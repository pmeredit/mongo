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
#include "ldap_connection.h"
#endif

namespace mongo {

StatusWith<std::unique_ptr<LDAPConnection>> LDAPConnectionFactory::create(
    const LDAPConnectionOptions& options) {
#ifdef _WIN32
    return Status(ErrorCodes::UnknownError, "LDAP connections are not supported on Windows.");
#else
    std::unique_ptr<LDAPConnection> client = stdx::make_unique<OpenLDAPConnection>(options);

    Status status = client->connect();
    if (!status.isOK()) {
        return status;
    }

    return std::move(client);
#endif
}
}  // namespace mongo
