/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/management/stream_manager.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/operation_context.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/commands/start_stream_processor_gen.h"
#include "streams/exec/executor.h"
#include "streams/exec/parser.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

StreamManager& StreamManager::get() {
    static auto streamManager = new StreamManager();
    return *streamManager;
}

void StreamManager::startStreamProcessor(std::string name,
                                         const std::vector<mongo::BSONObj>& pipeline,
                                         const std::vector<mongo::Connection>& connections) {
    stdx::lock_guard<Latch> lk(_mutex);

    uassert(ErrorCode::kTemporaryUserErrorCode,
            str::stream() << "streamProcessor name already exists: " << name,
            _processors.find(name) == _processors.end());

    Parser streamParser(connections);

    LOGV2_INFO(ErrorCode::kTemporaryLoggingCode, "Parsing", "name"_attr = name);
    LOGV2_INFO(75898, "Parsing", "name"_attr = name);
    StreamProcessorInfo processorInfo;
    processorInfo.operatorDag = streamParser.fromBson(name, pipeline);

    Executor::Options executorOptions;
    executorOptions.streamProcessorName = name;
    executorOptions.operatorDag = processorInfo.operatorDag.get();
    processorInfo.executor = std::make_unique<Executor>(std::move(executorOptions));

    _processors.emplace(std::make_pair(name, std::move(processorInfo)));

    LOGV2_INFO(75899, "Starting", "name"_attr = name);
    _processors[name].executor->start();
    LOGV2_INFO(75900, "Started", "name"_attr = name);
}

}  // namespace streams
