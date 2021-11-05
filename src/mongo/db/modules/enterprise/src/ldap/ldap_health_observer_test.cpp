/**
 *  Copyright (C) 2021-present MongoDB, Inc.
 */

#include "ldap_health_observer.h"
#include "ldap_manager_impl.h"
#include "ldap_options.h"

#include "mongo/db/process_health/fault_manager.h"
#include "mongo/db/process_health/fault_manager_config.h"
#include "mongo/executor/thread_pool_task_executor_test_fixture.h"
#include "mongo/logv2/log_component_settings.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/clock_source_mock.h"

namespace mongo {
namespace process_health {
namespace {

std::shared_ptr<executor::ThreadPoolTaskExecutor> constructTaskExecutor() {
    auto network = std::make_unique<executor::NetworkInterfaceMock>();
    auto executor = makeSharedThreadPoolTestExecutor(std::move(network));
    executor->startup();
    return executor;
}

/**
 * Test wrapper class for FaultManager that has access to protected methods
 * for testing.
 */
class FaultManagerTestImpl : public FaultManager {
public:
    FaultManagerTestImpl(ServiceContext* svcCtx,
                         std::shared_ptr<executor::TaskExecutor> taskExecutor)
        : FaultManager(
              svcCtx, taskExecutor, std::make_unique<FaultManagerConfig>(), [](std::string) {}) {}

    void transitionStateTest(FaultState newState) {
        transitionToState(newState);
    }

    FaultState getFaultStateTest() {
        return getFaultState();
    }

    void healthCheckTest() {
        healthCheck();
    }

    std::vector<HealthObserver*> getHealthObserversTest() {
        return getHealthObservers();
    }

    void processFaultExistsEventTest() {
        processFaultExistsEvent();
    }

    void processFaultIsResolvedEventTest() {
        processFaultIsResolvedEvent();
    }

    FaultInternal& getFault() {
        FaultFacetsContainerPtr fault = getFaultFacetsContainer();
        invariant(fault);
        return *(static_cast<FaultInternal*>(fault.get()));
    }
};

// Test suite for Ldap health observer.
class LdapHealthObserverTest : public unittest::Test {
public:
    void setUp() override {
        _svcCtx = ServiceContext::make();
        _svcCtx->setFastClockSource(std::make_unique<ClockSourceMock>());
        _svcCtx->setPreciseClockSource(std::make_unique<ClockSourceMock>());
        _taskExecutor = constructTaskExecutor();
        logv2::LogManager::global().getGlobalSettings().setMinimumLoggedSeverity(
            mongo::logv2::LogComponent::kProcessHealth, logv2::LogSeverity::Debug(3));
        logv2::LogManager::global().getGlobalSettings().setMinimumLoggedSeverity(
            mongo::logv2::LogComponent::kAccessControl, logv2::LogSeverity::Debug(3));
        resetManager();
        setStandardParams();
        resetLdapManager();

        // Lazy initialization requires that the health check is
        // triggered once before the health observers are instantiated.
        manager().healthCheckTest();
    }

    void tearDown() override {
        // Shutdown the executor before the context is deleted.
        resetManager();
    }

    void resetManager() {
        FaultManager::set(_svcCtx.get(),
                          std::make_unique<FaultManagerTestImpl>(_svcCtx.get(), _taskExecutor));
    }

    void resetLdapManager() {
        LDAPBindOptions bindOptions(globalLDAPParams->bindUser,
                                    globalLDAPParams->bindPassword,
                                    globalLDAPParams->bindMethod,
                                    globalLDAPParams->bindSASLMechanisms,
                                    globalLDAPParams->useOSDefaults);
        LDAPConnectionOptions connectionOptions(globalLDAPParams->connectionTimeout,
                                                globalLDAPParams->serverHosts);
        auto queryParameters =
            uassertStatusOK(LDAPQueryConfig::createLDAPQueryConfigWithUserNameAndAttributeTranform(
                globalLDAPParams->userAcquisitionQueryTemplate));
        auto mapper = uassertStatusOK(
            InternalToLDAPUserNameMapper::createNameMapper(globalLDAPParams->userToDNMapping));
        auto runner = std::make_unique<LDAPRunnerImpl>(bindOptions, connectionOptions);
        auto manager = std::make_unique<LDAPManagerImpl>(
            std::move(runner), std::move(queryParameters), std::move(mapper));
        LDAPManager::set(_svcCtx.get(), std::move(manager));
    }

    FaultManagerTestImpl& manager() {
        return *static_cast<FaultManagerTestImpl*>(FaultManager::get(_svcCtx.get()));
    }

    LdapHealthObserver& observer() {
        std::vector<HealthObserver*> observers = manager().getHealthObserversTest();
        ASSERT_TRUE(!observers.empty());
        auto it = std::find_if(observers.begin(), observers.end(), [](const HealthObserver* o) {
            return o->getType() == FaultFacetType::kLdap;
        });
        ASSERT_TRUE(it != observers.end());
        return *static_cast<LdapHealthObserver*>(*it);
    }

    LdapHealthObserver::PeriodicHealthCheckContext checkContext() {
        LdapHealthObserver::PeriodicHealthCheckContext ctx{CancellationToken::uncancelable(),
                                                           _taskExecutor};
        return ctx;
    }

    void setStandardParams() {
        if (!globalLDAPParams) {
            globalLDAPParams = new LDAPOptions();
        }
        globalLDAPParams->serverHosts = {
            LDAPHost(LDAPHost::Type::kDefault, "ldaptest.10gen.cc", false)};
        globalLDAPParams->bindMethod = LDAPBindType::kSimple;
        globalLDAPParams->bindUser = "cn=ldapz_admin,ou=Users,dc=10gen,dc=cc";
        globalLDAPParams->bindPassword = "Secret123";
        globalLDAPParams->transportSecurity = LDAPTransportSecurityType::kNone;
        globalLDAPParams->userAcquisitionQueryTemplate = "{USER}?memberOf";
        globalLDAPParams->connectionTimeout = Milliseconds(10000);
    }

    void addBadLdapServer() {
        globalLDAPParams->serverHosts.push_back(
            LDAPHost(LDAPHost::Type::kDefault, "badhost.10gen.cc", false));
    }

private:
    ServiceContext::UniqueServiceContext _svcCtx;
    std::shared_ptr<executor::ThreadPoolTaskExecutor> _taskExecutor;
};

TEST_F(LdapHealthObserverTest, HealthObserverIsLoaded) {
    std::vector<HealthObserver*> observers = manager().getHealthObserversTest();
    const int count =
        std::count_if(observers.begin(), observers.end(), [](const HealthObserver* o) {
            return o->getType() == FaultFacetType::kLdap;
        });
    ASSERT_EQ(1, count);
}

TEST_F(LdapHealthObserverTest, SmokeCheckIsSuccess) {
    for (auto value : {true, false}) {
        LdapHealthObserver::CheckResult result;
        result.smokeCheck = value;
        ASSERT_EQ(value, result.checkPassed());
    }
}

TEST_F(LdapHealthObserverTest, FullPeriodicCheck) {
    LdapHealthObserver::CheckResult result = observer().checkImpl(checkContext());
    ASSERT_TRUE(result.dnsResolution);
    ASSERT_TRUE(result.failures.empty()) << result.failures[0];
    // Smoke check was made.
    ASSERT_TRUE(result.smokeCheck);
    ASSERT_EQ(1, result.hostsTestedBySmokeCheck);
    ASSERT_TRUE(result.checkPassed());
    ASSERT_EQ(0.0, result.severity);
}

TEST_F(LdapHealthObserverTest, OneServerDNSLookupFailed) {
    addBadLdapServer();
    resetLdapManager();

    LdapHealthObserver::CheckResult result = observer().checkImpl(checkContext());
    ASSERT_FALSE(result.dnsResolution);
    ASSERT_FALSE(result.failures.empty());
    // This bool flag is set only if at least one server passed smoke check.
    ASSERT_TRUE(result.smokeCheck);
    ASSERT_TRUE(result.hostsTestedBySmokeCheck >= 1);
    ASSERT_TRUE(result.checkPassed());
    // Here severity is > 0 but it doesn't matter, status wins.
}

TEST_F(LdapHealthObserverTest, NoGoodServersConfigured) {
    globalLDAPParams->serverHosts.clear();
    addBadLdapServer();
    resetLdapManager();

    LdapHealthObserver::CheckResult result = observer().checkImpl(checkContext());
    ASSERT_FALSE(result.dnsResolution);
    // No servers to run smoke check on.
    ASSERT_FALSE(result.smokeCheck);
    ASSERT_EQ(0, result.hostsTestedBySmokeCheck);
    // The overall check is considered failed.
    ASSERT_FALSE(result.checkPassed());
    ASSERT_GT(result.severity, 0.0);
}

TEST_F(LdapHealthObserverTest, SmokeCheckPassedOnEmptyConfig) {
    globalLDAPParams->serverHosts.clear();
    resetLdapManager();

    LdapHealthObserver::CheckResult result = observer().checkImpl(checkContext());
    ASSERT_FALSE(result.dnsResolution);
    // Smoke check flag was set to true to signal success.
    ASSERT_TRUE(result.smokeCheck);
    ASSERT_EQ(0, result.hostsTestedBySmokeCheck);
    // Completely empty `serverHosts` makes it healthy - we
    // assume no Ldap was configured.
    ASSERT_TRUE(result.checkPassed());
}

TEST_F(LdapHealthObserverTest, SmokeCheckFailedOnBadConfig) {
    globalLDAPParams->bindUser = "malformed";
    resetLdapManager();
    LdapHealthObserver::CheckResult result = observer().checkImpl(checkContext());
    ASSERT_TRUE(result.dnsResolution);

    // Smoke check fails on malformed bind user.
    ASSERT_FALSE(result.failures.empty());
    ASSERT_FALSE(result.smokeCheck);
    ASSERT_EQ(1, result.hostsTestedBySmokeCheck);
    // Malformed servers list is a failure.
    ASSERT_FALSE(result.checkPassed());
    ASSERT_GT(result.severity, 0.0);
}

}  // namespace
}  // namespace process_health
}  // namespace mongo
