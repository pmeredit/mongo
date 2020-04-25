/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kExecutor

#include "mongo/platform/basic.h"

#include "mongot_task_executor.h"

#include <utility>

#include "mongo/executor/network_interface_factory.h"
#include "mongo/executor/network_interface_thread_pool.h"
#include "mongo/executor/thread_pool_task_executor.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace executor {

namespace {

struct State {
    State() : hasStarted(false) {
        std::shared_ptr<NetworkInterface> ni =
            makeNetworkInterface("MongotExecutor", nullptr, nullptr, {});
        auto tp = std::make_unique<NetworkInterfaceThreadPool>(ni.get());
        executor = std::make_unique<ThreadPoolTaskExecutor>(std::move(tp), std::move(ni));
    }

    AtomicWord<bool> hasStarted;
    std::unique_ptr<TaskExecutor> executor;
};

const auto getExecutor = ServiceContext::declareDecoration<State>();

ServiceContext::ConstructorActionRegisterer mongotExecutorCAR{
    "MongotTaskExecutor",
    [](ServiceContext* service) {},
    [](ServiceContext* service) {
        // Destruction implicitly performs the needed shutdown and join()
        getExecutor(service).executor.reset();
    }};

}  // namespace

TaskExecutor* getMongotTaskExecutor(ServiceContext* svc) {
    auto& state = getExecutor(svc);
    invariant(state.executor);

    if (!state.hasStarted.load() && !state.hasStarted.swap(true)) {
        state.executor->startup();
    }

    return state.executor.get();
}

}  // namespace executor
}  // namespace mongo
