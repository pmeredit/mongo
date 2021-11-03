/**
 *  Copyright (C) 2021-present MongoDB, Inc.
 */
#pragma once

#include "mongo/db/process_health/health_observer_base.h"

#include "mongo/db/service_context.h"

namespace mongo {
namespace process_health {

/**
 * Implementation of health observer for Ldap.
 */
class LdapHealthObserver final : public HealthObserverBase {
public:
    explicit LdapHealthObserver(ServiceContext* svcCtx);
    ~LdapHealthObserver() = default;

    /**
     * Health observer unique type.
     */
    FaultFacetType getType() const {
        return FaultFacetType::kLdap;
    }

    /**
     * Health check implementation.
     */
    Future<HealthCheckStatus> periodicCheckImpl(
        PeriodicHealthCheckContext&& periodicCheckContext) override;
};

}  // namespace process_health
}  // namespace mongo
