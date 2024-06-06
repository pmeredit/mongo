/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "ldap_manager.h"

#include <memory>

#include "mongo/base/init.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/db/service_context.h"

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

bool LDAPManager::useCyrusForAuthN() const {
    if (!saslGlobalParams.authdPath.empty()) {
        return true;
    }

    return getHosts().empty();
}
}  // namespace mongo
