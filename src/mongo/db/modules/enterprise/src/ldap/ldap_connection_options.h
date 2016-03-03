/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

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
