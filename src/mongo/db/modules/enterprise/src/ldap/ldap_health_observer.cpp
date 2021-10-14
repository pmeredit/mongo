/**
 *  Copyright (C) 2021-present MongoDB, Inc.
 */

#include "ldap_health_observer.h"

#include "mongo/db/process_health/health_observer_registration.h"
#include "mongo/db/service_context.h"

namespace mongo {
namespace process_health {

LdapHealthObserver::LdapHealthObserver(ClockSource* clockSource, TickSource* tickSource)
    : HealthObserverBase(clockSource, tickSource) {}

Future<HealthCheckStatus> LdapHealthObserver::periodicCheckImpl(
    PeriodicHealthCheckContext&& periodicCheckContext) {
    return Future<HealthCheckStatus>();
}

HealthObserverIntensity LdapHealthObserver::getIntensity() {
    return HealthObserverIntensity::kNonCritical;
}

namespace {

// Health observer registration.
MONGO_INITIALIZER(LdapHealthObserver)(InitializerContext*) {
    HealthObserverRegistration::registerObserverFactory(
        [](ClockSource* clockSource, TickSource* tickSource) {
            return std::make_unique<LdapHealthObserver>(clockSource, tickSource);
        });
}

}  // namespace

}  // namespace process_health
}  // namespace mongo
