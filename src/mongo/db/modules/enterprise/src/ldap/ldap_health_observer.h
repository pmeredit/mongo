/**
 *  Copyright (C) 2021-present MongoDB, Inc.
 */
#pragma once

#include "mongo/db/process_health/health_observer.h"

#include "mongo/db/service_context.h"

namespace mongo {
namespace process_health {

/**
 * Implementation of health observer for Ldap.
 */
class LdapHealthObserver : public HealthObserver {
public:
    LdapHealthObserver(ClockSource* clockSource);
    ~LdapHealthObserver() final = default;

    /**
     * Health observer unique type.
     */
    FaultFacetType getType() const final {
        return FaultFacetType::kLdap;
    }

    /**
     * Triggers health check.
     * It should be safe to invoke this method arbitrary often, the implementation
     * should prorate the invocations to avoid DoS.
     * The implementation may or may not block for the completion of the check, this remains
     * unspecified.
     *
     * @param factory Interface to get or create the factory of facets container.
     */
    void periodicCheck(FaultFacetsContainerFactory& factory) override;

    HealthObserverIntensity getIntensity() override;

private:
    ClockSource* const _clockSource;
};

}  // namespace process_health
}  // namespace mongo
