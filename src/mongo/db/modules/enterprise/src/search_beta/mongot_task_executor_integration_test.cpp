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

// Basic test that the mongot task executor is actually set up and works
TEST(MongotTaskExecutor, Basic) {
    ServiceContext::UniqueServiceContext svc = ServiceContext::make();
    auto exec = getMongotTaskExecutor(svc.get());

    RemoteCommandRequest rcr(unittest::getFixtureConnectionString().getServers().front(),
                             "admin",
                             BSON("isMaster" << 1),
                             BSONObj(),
                             nullptr);

    auto pf = makePromiseFuture<void>();

    ASSERT(exec->scheduleRemoteCommand(std::move(rcr),
                                       [&](const TaskExecutor::RemoteCommandCallbackArgs& args) {
                                           if (args.response.isOK()) {
                                               pf.promise.emplaceValue();
                                           } else {
                                               pf.promise.setError(args.response.status);
                                           }
                                       })
               .isOK());

    ASSERT_OK(pf.future.getNoThrow());
}

TEST(MongotTaskExecutor, NotUsingIsNonFatal) {
    // Test purposefully makes a service context and immediately throws it away to ensure that we
    // can construct and destruct a service context (which is decorated with a mongot  executor)
    // even if we never call startup().
    ServiceContext::make();
}

}  // namespace
}  // namespace executor
}  // namespace mongo
