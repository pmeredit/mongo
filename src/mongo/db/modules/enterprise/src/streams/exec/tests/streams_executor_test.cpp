/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <fmt/format.h>

#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/executor.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/planner.h"
#include "streams/exec/tests/test_utils.h"

namespace streams {

using namespace mongo;

class ExecutorTest : public AggregationContextFixture {
public:
    ExecutorTest() {
        _context = std::get<0>(getTestContext(/*svcCtx*/ nullptr));
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
    _context->connections = testInMemoryConnectionRegistry();
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
    executor->stop(StopReason::Shutdown);

    // Verify that Executor failed with timeout error because it took too long to stop.
    ASSERT_THROWS_WHAT(executorFuture.get(), DBException, "Timeout while stopping"_sd);

    // Deactivate the fail point.
    setGlobalFailPoint("streamProcessorStopSleepSeconds",
                       BSON("mode"
                            << "off"));
}

}  // namespace streams
