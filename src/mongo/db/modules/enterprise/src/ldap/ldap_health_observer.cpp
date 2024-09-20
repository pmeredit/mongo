/**
 *  Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "ldap_health_observer.h"
#include "ldap_manager.h"

#include "mongo/base/error_codes.h"
#include "mongo/base/string_data.h"
#include "mongo/db/process_health/health_observer_registration.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kProcessHealth


namespace mongo {
namespace process_health {

namespace {

// Use this many threads to run several smoke checks concurrently.
constexpr uint32_t kThreadConcurrency = 3;
constexpr double kDnsPenalty = 0.1;
constexpr double kSmokePenalty = 0.1;

}  // namespace

LdapHealthObserver::LdapHealthObserver(ServiceContext* svcCtx) : HealthObserverBase(svcCtx) {}

Future<HealthCheckStatus> LdapHealthObserver::periodicCheckImpl(
    PeriodicHealthCheckContext&& periodicCheckContext) {
    auto completionPf = makePromiseFuture<HealthCheckStatus>();

    auto result = checkImpl(periodicCheckContext);
    // If there was at least one host passing all tests return healthy status.
    if (result.checkPassed()) {
        completionPf.promise.emplaceValue(makeHealthyStatus());
    } else {
        completionPf.promise.emplaceValue(
            makeSimpleFailedStatus(Severity::kFailure, std::move(result.failures)));
    }

    return std::move(completionPf.future);
}

LdapHealthObserver::CheckResult LdapHealthObserver::checkImpl(
    const PeriodicHealthCheckContext& periodicCheckContext) {
    LdapHealthObserver::CheckResult result;
    try {
        if (globalLDAPParams->serverHosts.empty()) {
            static constexpr char kMsg[] =
                "No LDAP hosts configured but LDAP health check is enabled, use "
                "security.ldap.servers";
            LOGV2_DEBUG(5938601, 3, kMsg);
            // Effectively this aborts the check without error. We check params consistency
            // elsewhere.
            result.smokeCheck = true;
            invariant(result.checkPassed());
            return result;
        }

        // DNS checks are synchronous, block to result.
        std::shared_ptr<std::deque<LDAPHost>> resolvedHosts =
            std::make_shared<std::deque<LDAPHost>>(_checkDNS(&result));
        if (resolvedHosts->empty()) {
            return result;  // Error or Ok is set by `_checkDNS`.
        }
        // Shuffle hosts to avoid trying the same server every time.
        auto rng = std::default_random_engine{};
        std::shuffle(std::begin(*resolvedHosts), std::end(*resolvedHosts), rng);

        LDAPBindOptions bindOptions(globalLDAPParams->bindUser,
                                    globalLDAPParams->bindPassword,
                                    globalLDAPParams->bindMethod,
                                    globalLDAPParams->bindSASLMechanisms,
                                    globalLDAPParams->useOSDefaults);

        // Run smoke checks concurrently to avoid the case when a faulty server blocks the
        // progress.
        _runSmokeChecksConcurrently(
            periodicCheckContext, bindOptions, std::move(resolvedHosts), &result);
    } catch (const DBException& e) {
        result.smokeCheck = false;
        result.failures = {e.toStatus()};
    }

    return result;
}

bool LdapHealthObserver::isConfigured() const {
    if (globalLDAPParams->serverHosts.empty()) {
        static constexpr char kMsg[] =
            "No LDAP hosts configured but LDAP health check is enabled, use security.ldap.servers";
        LOGV2_DEBUG(5939100, 3, kMsg);
        return false;
    } else {
        return true;
    }
}

std::deque<LDAPHost> LdapHealthObserver::_checkDNS(LdapHealthObserver::CheckResult* result) {
    std::deque<LDAPHost> resolvedHosts;

    if (!_dnsCache || _lastCacheResetTimer.elapsed() > kResetDNSCacheInterval) {
        _lastCacheResetTimer.reset();
        _dnsCache = std::make_unique<LDAPDNSResolverCache>();
    }

    std::vector<std::string> failedHostsLog;
    for (const auto& ldapServer : globalLDAPParams->serverHosts) {
        auto currLookup = _dnsCache->resolve(ldapServer);
        if (currLookup.isOK()) {
            resolvedHosts.push_back(ldapServer);
            continue;
        }
        result->failures.push_back(currLookup.getStatus());
        result->severity += kDnsPenalty;
        failedHostsLog.push_back(ldapServer.toString());
    }

    if (resolvedHosts.size() == globalLDAPParams->serverHosts.size()) {
        LOGV2_DEBUG(5938602, 3, "Resolved all LDAP hosts", "resolved"_attr = resolvedHosts);
        result->dnsResolution = true;  // Every host was resolved.
    } else {
        LOGV2_DEBUG(5938603,
                    1,
                    "Did not resolve all LDAP hosts",
                    "resolved"_attr = resolvedHosts.size(),
                    "total"_attr = globalLDAPParams->serverHosts.size(),
                    "failedHosts"_attr = failedHostsLog);
    }

    return resolvedHosts;
}

namespace {
using PromiseFutureVector = std::vector<
    std::pair<Promise<LdapHealthObserver::CheckResult>, Future<LdapHealthObserver::CheckResult>>>;
using SharedPromiseFutureVector = std::shared_ptr<PromiseFutureVector>;
}  // namespace

// Each running thread owns its own copy of this struct.
struct LdapHealthObserver::ConcurrentRunContext {
    ConcurrentRunContext(const LDAPBindOptions& bindOptions,
                         const LdapHealthObserver::PeriodicHealthCheckContext& periodicCheckContext,
                         SharedPromiseFutureVector promisesAndFutures,
                         std::shared_ptr<std::deque<LDAPHost>> resolvedHosts,
                         ServiceContext* svcCtx,
                         TickSource* tickSource)
        : bindOptions(bindOptions),
          periodicCheckContext(periodicCheckContext),
          promisesAndFutures(promisesAndFutures),
          resolvedHosts(std::move(resolvedHosts)),
          svcCtx(svcCtx),
          tickSource(tickSource) {}
    const LDAPBindOptions bindOptions;
    LdapHealthObserver::PeriodicHealthCheckContext periodicCheckContext;
    // Vector itself is immutable, only futures values can change.
    SharedPromiseFutureVector promisesAndFutures;

    mutable stdx::mutex mutex;
    // Only this field needs protection, shared by all threads.
    std::shared_ptr<std::deque<LDAPHost>> resolvedHosts;
    ServiceContext* const svcCtx;
    TickSource* const tickSource;
};

void LdapHealthObserver::_runSmokeChecksConcurrently(
    const PeriodicHealthCheckContext& periodicCheckContext,
    const LDAPBindOptions& bindOptions,
    std::shared_ptr<std::deque<LDAPHost>> resolvedHosts,
    LdapHealthObserver::CheckResult* aggregateResult) {
    const auto concurrency = std::min<uint32_t>(kThreadConcurrency, resolvedHosts->size());
    // One promise-future per thread to watch for.
    SharedPromiseFutureVector promisesAndFutures = std::make_shared<PromiseFutureVector>();
    // A cancelation source to signal no more work is necessary - either a good server is found
    // or there are no more servers.
    // We also use this cancelation source to block until we can return from this method.
    CancellationSource completionCancellationSource(periodicCheckContext.cancellationToken);

    for (uint32_t threadNumber = 0; threadNumber < concurrency; ++threadNumber) {
        auto [promise, future] = makePromiseFuture<CheckResult>();
        promisesAndFutures->push_back(std::make_pair(std::move(promise), std::move(future)));
    }

    // Some checks can be stuck, the lifetime of 'concurrentRunContext' and everything else
    // captured by lambda can outlive this method invocation.
    auto concurrentRunContext = std::make_shared<ConcurrentRunContext>(bindOptions,
                                                                       periodicCheckContext,
                                                                       promisesAndFutures,
                                                                       resolvedHosts,
                                                                       svcCtx(),
                                                                       tickSource());

    for (uint32_t threadNumber = 0; threadNumber < concurrency; ++threadNumber) {
        concurrentRunContext->periodicCheckContext.taskExecutor->schedule(
            [concurrency, threadNumber, concurrentRunContext, completionCancellationSource](
                Status status) mutable {
                auto promise =
                    std::move((*concurrentRunContext->promisesAndFutures)[threadNumber].first);
                auto cancellationToken = completionCancellationSource.token();
                CheckResult perThreadResult;

                if (!status.isOK()) {
                    LOGV2(6131503, "Ldap smoke check aborted", "status"_attr = status);
                    promise.emplaceValue(std::move(perThreadResult));
                    completionCancellationSource.cancel();
                    return;
                }

                // Loops until we find a good host or get canceled.
                while (!cancellationToken.isCanceled() && !perThreadResult.smokeCheck) {
                    boost::optional<LDAPHost> host;
                    {
                        stdx::lock_guard lk(concurrentRunContext->mutex);
                        if (concurrentRunContext->resolvedHosts->empty()) {
                            break;  // No hosts left to check.
                        }
                        host = std::move(concurrentRunContext->resolvedHosts->front());
                        concurrentRunContext->resolvedHosts->pop_front();
                    }

                    std::vector<LDAPHost> oneHostVec{host.value()};
                    LDAPConnectionOptions connectionOptions(globalLDAPParams->connectionTimeout,
                                                            oneHostVec);
                    _smokeCheck(concurrentRunContext->bindOptions,
                                connectionOptions,
                                concurrentRunContext,
                                &perThreadResult);
                }

                const auto smokeCheckSuccess = perThreadResult.smokeCheck;
                promise.emplaceValue(std::move(perThreadResult));
                if (smokeCheckSuccess) {
                    // Signal to other threads that no more work is necessary.
                    completionCancellationSource.cancel();
                }

                // Check all futures. As every thread does this eventually the source is canceled
                // even though there is a subtle race.
                if (concurrency ==
                    std::count_if(concurrentRunContext->promisesAndFutures->begin(),
                                  concurrentRunContext->promisesAndFutures->end(),
                                  [](const auto& pf) { return pf.second.isReady(); })) {
                    completionCancellationSource.cancel();
                }
            });
    }

    completionCancellationSource.token().onCancel().waitNoThrow().ignore();

    // Merge all ready thread local results into aggregateResult.
    // This method can exit before all futures are ready. If the check is complete
    // at least one future is ready. However the periodicCheckContext.cancellationToken
    // may abort the execution before any result is received.
    // We may abandon results from some threads and it affects stats, but it's not important.
    int mergedThreads = 0;
    for (uint32_t threadNumber = 0; threadNumber < concurrency; ++threadNumber) {
        Future<CheckResult>& future = (*promisesAndFutures)[threadNumber].second;
        if (future.isReady()) {
            CheckResult result = std::move(future.get());
            aggregateResult->mergeFrom(result);
            ++mergedThreads;
        }
    }
    LOGV2_DEBUG(5939401,
                2,
                "LDAP smoke check completes",
                "mergedThreads"_attr = mergedThreads,
                "result"_attr = aggregateResult->smokeCheck);
}

void LdapHealthObserver::_smokeCheck(const LDAPBindOptions& bindOptions,
                                     const LDAPConnectionOptions& connectionOptions,
                                     std::shared_ptr<ConcurrentRunContext> runContext,
                                     CheckResult* result) try {
    Timer timer;
    invariant(!connectionOptions.hosts.empty());
    auto userAcquisitionStats = std::make_shared<UserAcquisitionStats>();
    // Avoid using the connection pool as a recycled connection will try to use
    // an already deleted copy of the 'userAcquisitionStats'.
    auto status = LDAPManager::get(runContext->svcCtx)
                      ->checkLivenessNotPooled(
                          connectionOptions, runContext->tickSource, userAcquisitionStats);

    result->hostsTestedBySmokeCheck++;
    if (!status.isOK()) {
        status =
            status.withContext(str::stream() << "Server " << connectionOptions.hosts[0].toString());
        result->failures.push_back(status);
        result->severity += kSmokePenalty;
        LOGV2_DEBUG(5938604,
                    1,
                    "LDAP smoke check failure",
                    "error"_attr = status,
                    "elapsedTimeMs"_attr = timer.millis());
        return;
    }
    // Will be set if at least one host passed smoke check.
    LOGV2_DEBUG(5938605, 3, "LDAP smoke check passed", "server"_attr = connectionOptions.hosts);
    result->smokeCheck = true;
} catch (const DBException& ex) {
    LOGV2_WARNING(6131501, "Ldap smoke check failed", "error"_attr = ex.toString());
} catch (...) {
    LOGV2_WARNING(6131502, "Ldap smoke check failed with unknown exception");
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
