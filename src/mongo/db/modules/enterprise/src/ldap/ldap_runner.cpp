/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_runner.h"

#include "mongo/base/init.h"
#include "mongo/db/service_context.h"
#include "mongo/stdx/memory.h"

namespace mongo {

namespace {
const auto getLDAPRunner = ServiceContext::declareDecoration<std::unique_ptr<LDAPRunner>>();
}  // namespace

void LDAPRunner::set(ServiceContext* service, std::unique_ptr<LDAPRunner> ldapRunner) {
    auto& runner = getLDAPRunner(service);
    invariant(ldapRunner);
    runner = std::move(ldapRunner);
}

LDAPRunner* LDAPRunner::get(ServiceContext* service) {
    return getLDAPRunner(service).get();
}
}  // namespace mongo
