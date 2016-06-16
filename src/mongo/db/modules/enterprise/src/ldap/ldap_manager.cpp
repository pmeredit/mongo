/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_manager.h"

#include "mongo/base/init.h"
#include "mongo/db/service_context.h"
#include "mongo/stdx/memory.h"

namespace mongo {

namespace {
const auto getLDAPManager = ServiceContext::declareDecoration<std::unique_ptr<LDAPManager>>();
}  // namespace

void LDAPManager::set(ServiceContext* service, std::unique_ptr<LDAPManager> ldapManager) {
    auto& manager = getLDAPManager(service);
    invariant(ldapManager);
    manager = std::move(ldapManager);
}

LDAPManager* LDAPManager::get(ServiceContext* service) {
    return getLDAPManager(service).get();
}
}  // namespace mongo
