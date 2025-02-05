/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <fmt/format.h>

#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/duration.h"
#include "streams/exec/executor.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/planner.h"
#include "streams/exec/sleeper.h"
#include "streams/exec/tests/test_utils.h"

namespace streams {

using namespace mongo;

class ExecutorTest : public AggregationContextFixture {
public:
    ExecutorTest() {
        _context = std::get<0>(getTestContext(/*svcCtx*/ nullptr));
    }

    Sleeper& getSleeper(Executor* executor) {
        return executor->_idleSleeper;
    }

protected:
    std::unique_ptr<Context> _context;
};

TEST_F(ExecutorTest, StopTimesOut) {
    // Set a failpoint to make Executor sleep for 15s while stopping.
    setGlobalFailPoint("streamProcessorStopSleepSeconds",
                       BSON("mode"
                            << "alwaysOn"
                            << "data" << BSON("sleepSeconds" << 15)));

    std::vector<BSONObj> rawPipeline{getTestSourceSpec(), getTestMemorySinkSpec()};
    _context->connections = std::make_unique<ConnectionCollection>(testInMemoryConnections());
    Planner planner(_context.get(), {});
    std::unique_ptr<OperatorDag> operatorDag = planner.plan(rawPipeline);

    Executor::Options options;
    options.operatorDag = operatorDag.get();
    options.stopTimeout = mongo::Seconds(10);
    options.metricManager = std::make_unique<MetricManager>();
    auto executor = std::make_unique<Executor>(_context.get(), std::move(options));

    // Start the Executor and wait until it is successfully connected.
    auto executorFuture = executor->start();
    while (!executor->isConnected()) {
        sleepFor(Milliseconds(100));
    }

    // Stop the Executor.
    executor->stop(StopReason::Shutdown, /*checkpointOnStop*/ true);

    // Verify that Executor failed with timeout error because it took too long to stop.
    ASSERT_THROWS_WHAT(executorFuture.get(), DBException, "Timeout while stopping"_sd);

    // Deactivate the fail point.
    setGlobalFailPoint("streamProcessorStopSleepSeconds",
                       BSON("mode"
                            << "off"));
}

TEST_F(ExecutorTest, IdleDelay) {
    Executor::Options options;
    options.metricManager = std::make_unique<MetricManager>();
    auto executor = std::make_unique<Executor>(_context.get(), std::move(options));
    auto& sleeper = getSleeper(executor.get());

    auto sleepAndValidate = [&](Milliseconds expectedSleep) {
        auto start = Date_t::now();
        sleeper.sleep();
        ASSERT((Date_t::now() - start) >= expectedSleep);
    };

    sleepAndValidate(Milliseconds{1});
    sleepAndValidate(Milliseconds{2});
    sleepAndValidate(Milliseconds{4});
    sleepAndValidate(Milliseconds{8});
    sleepAndValidate(Milliseconds{16});
    sleepAndValidate(Milliseconds{32});
    sleepAndValidate(Milliseconds{64});
    sleepAndValidate(Milliseconds{100});
    sleepAndValidate(Milliseconds{100});
    ASSERT_EQ(Milliseconds{100}, sleeper.getSleepTime());
    sleeper.reset();
    ASSERT_EQ(Milliseconds{1}, sleeper.getSleepTime());
}

}  // namespace streams
