/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "mongo/base/secure_allocator.h"
#include "mongo/util/time_support.h"

namespace mongo {

enum class LDAPBindType : std::uint8_t;

class LDAPOptions {
public:
    Milliseconds connectionTimeout;            // Duration after which connections shall fail
    std::string serverURI;                     // URI host component, (ldap|ldaps)://server(:port)
    std::string userAcquisitionQueryTemplate;  // LDAP query, with `{USER}' substitution token
    LDAPBindType bindMethod;                   // Bind method to use to authenticate, simple or SASL
    std::string bindUser;            // User DN to bind(authenticate) against on the LDAP server
    std::string bindSASLMechanisms;  // If binding with SASL, comma separated SASL mechanisms to use
    SecureString bindPassword;       // Password to bind with
    std::string userToDNMapping;     // JSON transformation from authentication name to DN

    bool isLDAPAuthzEnabled() const {
        return !userAcquisitionQueryTemplate.empty();
    }
};

extern LDAPOptions* globalLDAPParams;

}  // namespace mongo
