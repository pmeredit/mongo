/**
 *  Copyright (C) 2021-present MongoDB, Inc.
 */

#include "ldap_health_observer.h"

#include "mongo/db/process_health/health_observer_registration.h"
#include "mongo/db/service_context.h"

namespace mongo {
namespace process_health {

LdapHealthObserver::LdapHealthObserver(ClockSource* clockSource) : _clockSource(clockSource) {}

void LdapHealthObserver::periodicCheck(FaultFacetsContainerFactory& factory) {}

HealthObserverIntensity LdapHealthObserver::getIntensity() {
    return HealthObserverIntensity::kNonCritical;
}

namespace {

// Health observer registration.
MONGO_INITIALIZER(LdapHealthObserver)(InitializerContext*) {
    HealthObserverRegistration::registerObserverFactory(
        [](ClockSource* clockSource) { return std::make_unique<LdapHealthObserver>(clockSource); });
}

}  // namespace

}  // namespace process_health
}  // namespace mongo
