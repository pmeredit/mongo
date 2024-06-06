/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "mongo/base/secure_allocator.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/time_support.h"

#include "ldap_host.h"

namespace mongo {

enum class LDAPBindType : std::uint8_t;
enum class LDAPTransportSecurityType : std::uint8_t;

Status addMongodLDAPOptions(optionenvironment::OptionSection* options);
Status addSharedLDAPOptions(optionenvironment::OptionSection* options);

class LDAPOptions {
public:
    Milliseconds connectionTimeout;     // Duration after which connections shall fail
    int retryCount;                     // How many retries are allowed
    std::vector<LDAPHost> serverHosts;  // List of URI host components of form 'server(:port)'
    LDAPTransportSecurityType transportSecurity;  // How connections to the LDAP server are secured
    std::string userAcquisitionQueryTemplate;     // LDAP query, with `{USER}' substitution token
    bool useOSDefaults;              // Use the OS's default user when binding to remote LDAP server
    LDAPBindType bindMethod;         // Bind method to use to authenticate, simple or SASL
    std::string bindUser;            // User DN to bind(authenticate) against on the LDAP server
    std::string bindSASLMechanisms;  // If binding with SASL, comma separated SASL mechanisms to use
    std::string serverCAFile;        // path to CA certificate for TLS
    SecureString bindPassword;       // Password to bind with
    std::string userToDNMapping;     // JSON transformation from authentication name to DN
    bool smokeTestOnStartup;         // Verify the remote LDAP server is online when we start up

    bool isLDAPAuthzEnabled() const {
        return !userAcquisitionQueryTemplate.empty();
    }
};

extern LDAPOptions* globalLDAPParams;

}  // namespace mongo
