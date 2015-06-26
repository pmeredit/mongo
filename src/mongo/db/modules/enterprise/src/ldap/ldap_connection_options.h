/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <string>

#include "mongo/base/secure_allocator.h"
#include "mongo/util/time_support.h"

#include "ldap_bind_type.h"

namespace mongo {

/**
 * Contains all parameters required to bind to an LDAP server.
 */
struct LDAPBindOptions {
    LDAPBindOptions(std::string bindDN,
                    SecureString password,
                    LDAPBindType authenticationChoice,
                    std::string saslMechanisms)
        : bindDN(std::move(bindDN)),
          password(std::move(password)),
          authenticationChoice(authenticationChoice),
          saslMechanisms(std::move(saslMechanisms)) {}

    std::string bindDN;                 // The username, or entity DN, to bind with
    SecureString password;              // The password to bind with
    LDAPBindType authenticationChoice;  // The authentication system to use, simple or SASL
    std::string saslMechanisms;  // If authenticating with SASL, the SASL mechanism to bind with

    std::string toCleanString() const;
};

/**
 * Contains all parameters, beyond those defining a query, needed for an LDAP session.
 */
struct LDAPConnectionOptions {
    LDAPConnectionOptions(Milliseconds timeout, std::string hostURI)
        : timeout(std::move(timeout)), hostURI(std::move(hostURI)) {}

    Milliseconds timeout;  // How long to wait before timing out
    std::string hostURI;   // URI of the server: (ldap|ldaps)://(server)(:port)
};
}  // namespace mongo
