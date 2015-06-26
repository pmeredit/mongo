/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <string>
#include <vector>

#include "mongo/platform/unordered_map.h"

namespace mongo {
using LDAPDN = std::string;
using LDAPDNVector = std::vector<std::string>;
using LDAPAttributeKey = std::string;
using LDAPAttributeKeys = std::vector<LDAPAttributeKey>;
using LDAPAttributeValue = std::string;
using LDAPAttributeValues = std::vector<LDAPAttributeValue>;
using LDAPAttributeKeyValuesMap = std::unordered_map<LDAPAttributeKey, LDAPAttributeValues>;
using LDAPEntityCollection = std::unordered_map<LDAPDN, LDAPAttributeKeyValuesMap>;
}  // namespace mongo
