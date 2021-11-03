/**
 *  Copyright (C) 2021-present MongoDB, Inc.
 */

#include "ldap_health_observer.h"

#include "mongo/db/process_health/health_observer_registration.h"
#include "mongo/db/service_context.h"

namespace mongo {
namespace process_health {

LdapHealthObserver::LdapHealthObserver(ServiceContext* svcCtx) : HealthObserverBase(svcCtx) {}

Future<HealthCheckStatus> LdapHealthObserver::periodicCheckImpl(
    PeriodicHealthCheckContext&& periodicCheckContext) {
    return Future<HealthCheckStatus>();
}

namespace {

// Health observer registration.
MONGO_INITIALIZER(LdapHealthObserver)(InitializerContext*) {
    HealthObserverRegistration::registerObserverFactory(
        [](ServiceContext* svcCtx) { return std::make_unique<LdapHealthObserver>(svcCtx); });
}

}  // namespace

}  // namespace process_health
}  // namespace mongo
