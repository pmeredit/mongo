/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <cstdint>
#include <string>

#include "mongo/base/secure_allocator.h"
#include "mongo/util/time_support.h"

namespace mongo {

template <typename T>
class StatusWith;
class StringData;

/**
 * RFC4511 section 4.2 defines how clients may 'Bind', or authenticate themselves to servers.
 * This process allows the clients to specify an 'LDAPBindType', which is the
 * authentication scheme to use.
 * There are two options, which are represented by this enum:
 *  kSIMPLE, simple authentication, where the client presents a plaintext password to the server
 *  kSASL, SASL authentication, where the client uses a SASL mechanism to authenticate to the server
 */
enum class LDAPBindType : std::uint8_t { kSimple, kSasl };


enum class LDAPTransportSecurityType : std::uint8_t { kNone, kTLS };

/**
 * Parse an LDAPBindType from a string.
 * Will perform the following mapping:
 *  "simple" -> kSIMPLE
 *  "sasl" -> kSASL
 *  _ -> Error: FailedToParse
 */
StatusWith<LDAPBindType> getLDAPBindType(StringData type);

/** Produce the corresponding string representation of an LDAPBindType */
const StringData authenticationChoiceToString(LDAPBindType type);


/**
 * Contains all parameters required to bind to an LDAP server.
 */
struct LDAPBindOptions {
    LDAPBindOptions(std::string bindDN,
                    SecureString password,
                    LDAPBindType authenticationChoice,
                    std::string saslMechanisms,
                    bool useLDAPConnectionDefaults)
        : bindDN(std::move(bindDN)),
          password(std::move(password)),
          authenticationChoice(authenticationChoice),
          saslMechanisms(std::move(saslMechanisms)),
          useLDAPConnectionDefaults(useLDAPConnectionDefaults) {}

    LDAPBindOptions() = default;

    std::string bindDN;                 // The username, or entity DN, to bind with
    SecureString password;              // The password to bind with
    LDAPBindType authenticationChoice;  // The authentication system to use, simple or SASL
    std::string saslMechanisms;      // If authenticating with SASL, the SASL mechanism to bind with
    bool useLDAPConnectionDefaults;  // On Windows, and if true, ignore the bindDN and password and
                                     // use the service account's credentials
    bool shouldBind() const {
        return useLDAPConnectionDefaults || !bindDN.empty();
    }

    std::string toCleanString() const;
};

/**
 * Contains all parameters, beyond those defining a query, needed for an LDAP session.
 */
struct LDAPConnectionOptions {
    LDAPConnectionOptions(Milliseconds timeout,
                          std::vector<std::string> hosts,
                          LDAPTransportSecurityType transportSecurity)
        : timeout(std::move(timeout)),
          hosts(std::move(hosts)),
          transportSecurity(transportSecurity) {}

    LDAPConnectionOptions() = default;

    /**
     * Accepts a comma separated list of FQDNs and parses it into vector of FQDNs. FQDNs are
     * checked to ensure they do not have protocol prefixes.
     */
    static StatusWith<std::vector<std::string>> parseHostURIs(const std::string& hosts);

    /**
     * Returns a comma separated list of (protocol)://(fqdn)s, where the protocol is derived from
     * the value of 'transportSecurity', and the FQDNs are taken from 'hosts'.
     */
    StatusWith<std::string> constructHostURIs() const;

    Milliseconds timeout;                         // How long to wait before timing out
    std::vector<std::string> hosts;               // List of server URIs: (server)(:port)
    LDAPTransportSecurityType transportSecurity;  // How to secure connections to the LDAP server
    bool usePooledConnection = false;             // Whether to use the connection pool
};
}  // namespace mongo
