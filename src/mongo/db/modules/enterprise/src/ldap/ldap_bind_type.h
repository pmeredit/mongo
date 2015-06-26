/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <cstdint>

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

}  // namespace mongo
