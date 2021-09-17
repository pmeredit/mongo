/**
 *  Copyright (C) 2021-present MongoDB, Inc.
 */

#include "ldap_health_observer.h"

#include "mongo/db/process_health/fault_manager.h"
#include "mongo/executor/thread_pool_task_executor_test_fixture.h"
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
        : FaultManager(svcCtx, taskExecutor) {}

    Status transitionStateTest(FaultState newState) {
        return transitionToState(newState);
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

    Status processFaultExistsEventTest() {
        return processFaultExistsEvent();
    }

    Status processFaultIsResolvedEventTest() {
        return processFaultIsResolvedEvent();
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
        _taskExecutor = constructTaskExecutor();
        resetManager();
    }

    void resetManager() {
        FaultManager::set(_svcCtx.get(),
                          std::make_unique<FaultManagerTestImpl>(_svcCtx.get(), _taskExecutor));
    }

    FaultManagerTestImpl& manager() {
        return *static_cast<FaultManagerTestImpl*>(FaultManager::get(_svcCtx.get()));
    }

private:
    ServiceContext::UniqueServiceContext _svcCtx;
    std::shared_ptr<executor::ThreadPoolTaskExecutor> _taskExecutor;
};

TEST_F(LdapHealthObserverTest, HealthObserverIsLoaded) {
    // Lazy initialization requires that the health check is
    // triggered once before the health observers are instantiated.
    manager().healthCheckTest();

    std::vector<HealthObserver*> observers = manager().getHealthObserversTest();
    const int count =
        std::count_if(observers.begin(), observers.end(), [](const HealthObserver* o) {
            return o->getType() == FaultFacetType::kLdap;
        });
    ASSERT_EQ(1, count);
}

}  // namespace
}  // namespace process_health
}  // namespace mongo
