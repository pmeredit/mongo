/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */


#include "mongo/platform/basic.h"

#include "search_task_executors.h"

#include <utility>

#include "mongo/executor/network_interface_factory.h"
#include "mongo/executor/network_interface_thread_pool.h"
#include "mongo/executor/thread_pool_task_executor.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/util/assert_util.h"

#include "mongot_options.h"
#include "search_index_options.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kExecutor


namespace mongo {
namespace executor {

namespace {

struct State {
    State() : mongotExecStarted(false), searchIndexMgmtExecStarted(false) {
        // Make the MongotExecutor and associated NetworkInterface.
        ConnectionPool::Options mongotOptions;
        mongotOptions.skipAuthentication = globalMongotParams.skipAuthToMongot;
        auto mongotExecutorNetworkInterface =
            makeNetworkInterface("MongotExecutor", nullptr, nullptr, std::move(mongotOptions));
        auto mongotThreadPool =
            std::make_unique<NetworkInterfaceThreadPool>(mongotExecutorNetworkInterface.get());
        mongotExecutor = std::make_shared<ThreadPoolTaskExecutor>(
            std::move(mongotThreadPool), std::move(mongotExecutorNetworkInterface));

        // Make a separate searchIndexMgmtExecutor that's independently configurable.
        ConnectionPool::Options searchIndexPoolOptions;
        searchIndexPoolOptions.skipAuthentication =
            globalSearchIndexParams.skipAuthToSearchIndexServer;
        std::shared_ptr<NetworkInterface> searchIdxNI = makeNetworkInterface(
            "SearchIndexMgmtExecutor", nullptr, nullptr, std::move(searchIndexPoolOptions));
        auto searchIndexThreadPool =
            std::make_unique<NetworkInterfaceThreadPool>(searchIdxNI.get());
        searchIndexMgmtExecutor = std::make_shared<ThreadPoolTaskExecutor>(
            std::move(searchIndexThreadPool), std::move(searchIdxNI));
    }

    auto getMongotExecutorPtr() {
        invariant(mongotExecutor);

        if (!mongotExecStarted.load() && !mongotExecStarted.swap(true)) {
            mongotExecutor->startup();
        }
        return mongotExecutor;
    }

    auto getSearchIndexMgmtExecutorPtr() {
        invariant(searchIndexMgmtExecutor);

        if (!searchIndexMgmtExecStarted.load() && !searchIndexMgmtExecStarted.swap(true)) {
            searchIndexMgmtExecutor->startup();
        }
        return searchIndexMgmtExecutor;
    }

    AtomicWord<bool> mongotExecStarted;
    std::shared_ptr<TaskExecutor> mongotExecutor;

    AtomicWord<bool> searchIndexMgmtExecStarted;
    std::shared_ptr<TaskExecutor> searchIndexMgmtExecutor;
};

const auto getExecutorHolder = ServiceContext::declareDecoration<State>();

ServiceContext::ConstructorActionRegisterer searchExecutorsCAR{
    "SearchTaskExecutors",
    [](ServiceContext* service) {},
    [](ServiceContext* service) {
        // Destruction implicitly performs the needed shutdown and join()
        getExecutorHolder(service).mongotExecutor.reset();
        getExecutorHolder(service).searchIndexMgmtExecutor.reset();
    }};

}  // namespace

std::shared_ptr<TaskExecutor> getMongotTaskExecutor(ServiceContext* svc) {
    auto& state = getExecutorHolder(svc);
    invariant(state.mongotExecutor);
    return state.getMongotExecutorPtr();
}

std::shared_ptr<TaskExecutor> getSearchIndexManagementTaskExecutor(ServiceContext* svc) {
    auto& state = getExecutorHolder(svc);
    invariant(state.searchIndexMgmtExecutor);
    return state.getSearchIndexMgmtExecutorPtr();
}

}  // namespace executor
}  // namespace mongo
