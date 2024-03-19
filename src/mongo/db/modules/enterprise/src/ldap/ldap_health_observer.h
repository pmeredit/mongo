/**
 *  Copyright (C) 2021-present MongoDB, Inc.
 */
#pragma once

#include <deque>
#include <vector>

#include "mongo/db/process_health/health_observer_base.h"

#include "ldap_connection_options.h"
#include "ldap_options.h"
#include "ldap_resolver_cache.h"

#include "mongo/base/counter.h"
#include "mongo/db/service_context.h"
#include "mongo/util/future.h"
#include "mongo/util/timer.h"

namespace mongo {
namespace process_health {

// Even though the LDAPDNSResolverCache respects TTL we reset
// the cache according to this interval to force DNS resolution.
// The health check machinery handles transient failures properly.
static constexpr auto kResetDNSCacheInterval{Minutes(10)};

/**
 * Implementation of health observer for Ldap.
 */
class LdapHealthObserver final : public HealthObserverBase {
public:
    using HealthObserverBase::PeriodicHealthCheckContext;

    explicit LdapHealthObserver(ServiceContext* svcCtx);
    ~LdapHealthObserver() override = default;

    /**
     * Health observer unique type.
     */
    FaultFacetType getType() const override {
        return FaultFacetType::kLdap;
    }

    Milliseconds getObserverTimeout() const override {
        return Milliseconds(Seconds(30));
    }

    /**
     * Triggers health check.
     * It is guaranteed that the next check is never invoked until the promise for the
     * previous one is filled, thus synchronization can be relaxed.
     */
    Future<HealthCheckStatus> periodicCheckImpl(
        PeriodicHealthCheckContext&& periodicCheckContext) override;

    // Non interface methods exposed for direct testing.

    struct CheckResult {
        // Final criteria that the health check passed.
        bool checkPassed() const {
            return smokeCheck;
        }

        void mergeFrom(const CheckResult& r) {
            dnsResolution |= r.dnsResolution;
            hostsTestedBySmokeCheck += r.hostsTestedBySmokeCheck;
            smokeCheck |= r.smokeCheck;
            severity += r.severity;
            std::copy(r.failures.begin(), r.failures.end(), std::back_inserter(failures));
        }

        bool dnsResolution = false;
        int hostsTestedBySmokeCheck = 0;

        // The check passed if at least one Ldap server passed all tests,
        // or the `security.ldap.servers` is empty string.
        bool smokeCheck = false;

        double severity = 0;
        std::vector<Status> failures;
    };

    // Implementation of the health check, made public for testing.
    CheckResult checkImpl(const PeriodicHealthCheckContext& periodicCheckContext);

    bool isConfigured() const override;

private:
    struct ConcurrentRunContext;
    std::deque<LDAPHost> _checkDNS(CheckResult* result);

    // Wrapper to run smoke checks concurrently.
    void _runSmokeChecksConcurrently(const PeriodicHealthCheckContext& periodicCheckContext,
                                     const LDAPBindOptions& bindOptions,
                                     std::shared_ptr<std::deque<LDAPHost>> resolvedHosts,
                                     CheckResult* result);

    // Smoke check of one server, to be dispatched concurrently.
    static void _smokeCheck(const LDAPBindOptions& bindOptions,
                            const LDAPConnectionOptions& connectionOptions,
                            std::shared_ptr<ConcurrentRunContext> runContext,
                            CheckResult* result);

    // The fields that do not need synchronization as only one periodicCheckImpl()
    // can run concurrently.

    std::unique_ptr<LDAPDNSResolverCache> _dnsCache{new LDAPDNSResolverCache};

    Timer _lastCacheResetTimer;
};

}  // namespace process_health
}  // namespace mongo
