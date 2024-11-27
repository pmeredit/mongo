/**
 *  Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "ldap_health_observer.h"
#include "ldap_manager_impl.h"
#include "ldap_options.h"

#include "mongo/db/process_health/fault_manager.h"
#include "mongo/db/process_health/fault_manager_config.h"
#include "mongo/db/process_health/fault_manager_test_suite.h"
#include "mongo/executor/network_interface_factory.h"
#include "mongo/executor/thread_pool_task_executor_test_fixture.h"
#include "mongo/logv2/log_component_settings.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/clock_source_mock.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/tick_source_mock.h"

namespace mongo {
namespace process_health {
namespace {

using test::FaultManagerTest;

// Only covers negative test cases that don't rely on an actual, live LDAP server.
// See ldap_mongos_health_checking.js for positive test cases via an integration test.
class LdapHealthObserverTest : public FaultManagerTest {
public:
    void setUp() override {
        advanceTime(Seconds(100));
        bumpUpLogging();
        resetManager();

        setStandardParamsWithBadLdapServer();
        resetLdapManager();
        resetManager(std::make_unique<FaultManagerConfig>());

        // Lazy initialization requires that the health check is
        // triggered once before the health observers are instantiated.
        auto initialHealthCheckFuture = manager().startPeriodicHealthChecks();
        initialHealthCheckFuture.get();
    }

    void tearDown() override {
        auto reaperDestroyedFp = globalFailPointRegistry().find("ldapConnectionReaperDestroyed");
        auto timesEnteredBefore = reaperDestroyedFp->setMode(FailPoint::alwaysOn);
        // Force the existing manager to be cleaned and wait for the connection reaper
        // to be destroyed, because the reaper can outlive the manager and corrupt memory.
        resetLdapManager();
        reaperDestroyedFp->waitForTimesEntered(timesEnteredBefore + 1);
        reaperDestroyedFp->setMode(FailPoint::off);
        FaultManagerTest::tearDown();
    }

    void resetLdapManager() {
        LDAPBindOptions bindOptions(globalLDAPParams->bindUser,
                                    globalLDAPParams->bindPassword,
                                    globalLDAPParams->bindMethod,
                                    globalLDAPParams->bindSASLMechanisms,
                                    globalLDAPParams->useOSDefaults);
        auto connectionOptions = LDAPConnectionOptions(globalLDAPParams->connectionTimeout,
                                                       globalLDAPParams->serverHosts);
        auto factory = std::make_unique<LDAPConnectionFactory>(connectionOptions.timeout);
        auto queryParameters =
            uassertStatusOK(LDAPQueryConfig::createLDAPQueryConfigWithUserNameAndAttributeTranform(
                globalLDAPParams->userAcquisitionQueryTemplate));
        auto mapper = uassertStatusOK(
            InternalToLDAPUserNameMapper::createNameMapper(globalLDAPParams->userToDNMapping));
        auto runner =
            std::make_unique<LDAPRunnerImpl>(bindOptions, connectionOptions, std::move(factory));
        auto manager = std::make_unique<LDAPManagerImpl>(
            std::move(runner), std::move(queryParameters), std::move(mapper));
        LDAPManager::set(getServiceContext(), std::move(manager));
    }

    LdapHealthObserver& observer() {
        return FaultManagerTest::observer<LdapHealthObserver>(FaultFacetType::kLdap);
    }

    void setStandardParamsWithBadLdapServer() {
        if (!globalLDAPParams) {
            globalLDAPParams = new LDAPOptions();
        }
        globalLDAPParams->serverHosts = {
            LDAPHost(LDAPHost::Type::kDefault, "badhost.10gen.cc", false)};
        globalLDAPParams->bindMethod = LDAPBindType::kSimple;
        globalLDAPParams->bindUser = "cn=ldapz_admin,ou=Users,dc=10gen,dc=cc";
        globalLDAPParams->bindPassword = "Secret123";
        globalLDAPParams->transportSecurity = LDAPTransportSecurityType::kNone;
        globalLDAPParams->userAcquisitionQueryTemplate = "{USER}?memberOf";
        globalLDAPParams->connectionTimeout = Milliseconds(10000);
    }
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

TEST_F(LdapHealthObserverTest, NoGoodServersConfigured) {
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

}  // namespace
}  // namespace process_health
}  // namespace mongo
