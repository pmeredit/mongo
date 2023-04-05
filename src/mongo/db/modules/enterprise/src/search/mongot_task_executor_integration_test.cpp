/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongot_task_executor.h"

#include "mongo/db/service_context.h"
#include "mongo/unittest/integration_test.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/future.h"

namespace mongo {
namespace executor {
namespace {

class MongotTaskExecutorTest : public unittest::Test {
public:
    void setUp() override {
        _serviceCtx = ServiceContext::make();
        _executor = getMongotTaskExecutor(_serviceCtx.get());
    }

    void tearDown() override {
        _serviceCtx.reset();
    }

    auto executor() {
        return _executor;
    }

    auto makeRCR() const {
        return RemoteCommandRequest(unittest::getFixtureConnectionString().getServers().front(),
                                    "admin",
                                    BSON("isMaster" << 1),
                                    BSONObj(),
                                    nullptr);
    }

protected:
    ServiceContext::UniqueServiceContext _serviceCtx;
    std::shared_ptr<TaskExecutor> _executor;
};

class PinnedMongotTaskExecutorTest : public MongotTaskExecutorTest {
public:
    void setUp() override {
        MongotTaskExecutorTest::setUp();
        _pinnedExec = makePinnedConnectionTaskExecutor(_serviceCtx.get());
    }

    auto pinnedExec() {
        return _pinnedExec;
    }

private:
    std::shared_ptr<TaskExecutor> _pinnedExec;
};

// Basic test that the mongot task executor is actually set up and works
TEST_F(MongotTaskExecutorTest, Basic) {
    auto pf = makePromiseFuture<void>();

    ASSERT_OK(executor()->scheduleRemoteCommand(
        makeRCR(), [&](const TaskExecutor::RemoteCommandCallbackArgs& args) {
            pf.promise.setWith([&] { return args.response.status; });
        }));

    ASSERT_OK(pf.future.getNoThrow());
}

TEST(MongotTaskExecutor, NotUsingIsNonFatal) {
    // Test purposefully makes a service context and immediately throws it away to ensure that we
    // can construct and destruct a service context (which is decorated with a mongot  executor)
    // even if we never call startup().
    ServiceContext::make();
}

TEST_F(PinnedMongotTaskExecutorTest, RunSingleCommandOverPinnedConnection) {
    auto pf = makePromiseFuture<void>();

    ASSERT_OK(pinnedExec()->scheduleRemoteCommand(
        makeRCR(), [&](const TaskExecutor::RemoteCommandCallbackArgs& args) {
            pf.promise.setWith([&] { return args.response.status; });
        }));

    ASSERT_OK(pf.future.getNoThrow());
}

TEST_F(PinnedMongotTaskExecutorTest, RunMultipleCommandsOverPinnedConnection) {
    constexpr size_t numRequests = 2;
    std::vector<Future<void>> results;

    for (size_t i = 0; i < numRequests; ++i) {
        auto promise = std::make_shared<Promise<void>>(NonNullPromiseTag{});
        results.push_back(promise->getFuture());
        ASSERT_OK(pinnedExec()->scheduleRemoteCommand(
            makeRCR(),
            [p = std::move(promise)](const TaskExecutor::RemoteCommandCallbackArgs& args) {
                p->setWith([&] { return args.response.status; });
            }));
    }

    ASSERT_EQ(numRequests, results.size());
    for (size_t i = 0; i < numRequests; ++i) {
        ASSERT_OK(results[i].getNoThrow());
    }
}

}  // namespace
}  // namespace executor
}  // namespace mongo
