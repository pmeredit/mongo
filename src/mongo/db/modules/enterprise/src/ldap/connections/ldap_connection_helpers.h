/**
 *  Copyright (C) 2016 MongoDB Inc.
 */
#pragma once

#include <cstdint>

namespace mongo {

enum class LDAPQueryScope : std::uint8_t;
class Status;
class StringData;

// Transform an LDAPQueryScope into the corresponding library specific constant
int mapScopeToLDAP(const LDAPQueryScope& type);

}  // namespace mongo
