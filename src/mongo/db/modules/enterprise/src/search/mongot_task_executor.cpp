/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */


#include "mongo/platform/basic.h"

#include "mongot_task_executor.h"

#include <utility>

#include "mongo/executor/network_interface_factory.h"
#include "mongo/executor/network_interface_thread_pool.h"
#include "mongo/executor/thread_pool_task_executor.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/util/assert_util.h"

#include "mongot_options.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kExecutor


namespace mongo {
namespace executor {

namespace {

struct State {
    State() : hasStarted(false) {
        ConnectionPool::Options options;
        options.skipAuthentication = globalMongotParams.skipAuthToMongot;
        std::shared_ptr<NetworkInterface> ni =
            makeNetworkInterface("MongotExecutor", nullptr, nullptr, std::move(options));
        auto tp = std::make_unique<NetworkInterfaceThreadPool>(ni.get());
        net = ni;
        executor = std::make_shared<ThreadPoolTaskExecutor>(std::move(tp), std::move(ni));
    }

    auto getExecutorPtr() {
        invariant(executor);

        if (!hasStarted.load() && !hasStarted.swap(true)) {
            executor->startup();
        }
        return executor;
    }

    AtomicWord<bool> hasStarted;
    std::shared_ptr<TaskExecutor> executor;
    std::shared_ptr<NetworkInterface> net;
};

const auto getExecutorHolder = ServiceContext::declareDecoration<State>();

ServiceContext::ConstructorActionRegisterer mongotExecutorCAR{
    "MongotTaskExecutor",
    [](ServiceContext* service) {},
    [](ServiceContext* service) {
        // Destruction implicitly performs the needed shutdown and join()
        getExecutorHolder(service).executor.reset();
    }};

}  // namespace

std::shared_ptr<TaskExecutor> getMongotTaskExecutor(ServiceContext* svc) {
    auto& state = getExecutorHolder(svc);
    invariant(state.executor);
    return state.getExecutorPtr();
}

std::shared_ptr<PinnedConnectionTaskExecutor> makePinnedConnectionTaskExecutor(
    ServiceContext* svc) {
    auto& state = getExecutorHolder(svc);
    invariant(state.executor);
    return std::make_shared<PinnedConnectionTaskExecutor>(state.getExecutorPtr(), state.net.get());
}

}  // namespace executor
}  // namespace mongo
