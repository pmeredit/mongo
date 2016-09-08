/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <string>
#include <vector>

#include "mongo/stdx/unordered_map.h"

namespace mongo {
using LDAPDN = std::string;
using LDAPDNVector = std::vector<std::string>;
using LDAPAttributeKey = std::string;
using LDAPAttributeKeys = std::vector<LDAPAttributeKey>;
using LDAPAttributeValue = std::string;
using LDAPAttributeValues = std::vector<LDAPAttributeValue>;
using LDAPAttributeKeyValuesMap = stdx::unordered_map<LDAPAttributeKey, LDAPAttributeValues>;
using LDAPEntityCollection = stdx::unordered_map<LDAPDN, LDAPAttributeKeyValuesMap>;
}  // namespace mongo
