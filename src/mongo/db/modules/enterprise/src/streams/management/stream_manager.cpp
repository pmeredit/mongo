/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/management/stream_manager.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/operation_context.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/commands/start_stream_processor_gen.h"
#include "streams/exec/context.h"
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

    auto context = std::make_unique<Context>();
    context->streamName = name;
    context->clientName = name + "-" + UUID::gen().toString();
    context->client = getGlobalServiceContext()->makeClient(context->clientName);
    context->opCtx = getGlobalServiceContext()->makeOperationContext(context->client.get());
    // TODO(STREAMS-219)-PrivatePreview: We should make sure we're constructing the context
    // appropriately here
    context->expCtx = make_intrusive<ExpressionContext>(
        context->opCtx.get(), std::unique_ptr<CollatorInterface>(nullptr), NamespaceString{});
    context->expCtx->allowDiskUse = false;
    // TODO(STREAMS-219)-PrivatePreview: Considering exposing this as a parameter.
    // Or, set a parameter to dis-allow spilling.
    // We're using the same default as in run_aggregate.cpp.
    // This tempDir is used for spill to disk in $sort, $group, etc. stages
    // in window inner pipelines.
    context->expCtx->tempDir = storageGlobalParams.dbpath + "/_tmp";

    StreamProcessorInfo processorInfo;
    processorInfo.context = std::move(context);

    Parser streamParser(processorInfo.context.get(), connections);

    LOGV2_INFO(ErrorCode::kTemporaryLoggingCode, "Parsing", "name"_attr = name);
    LOGV2_INFO(75898, "Parsing", "name"_attr = name);
    processorInfo.operatorDag = streamParser.fromBson(pipeline);

    Executor::Options executorOptions;
    executorOptions.streamProcessorName = name;
    executorOptions.operatorDag = processorInfo.operatorDag.get();
    processorInfo.executor = std::make_unique<Executor>(std::move(executorOptions));

    _processors.emplace(std::make_pair(name, std::move(processorInfo)));

    LOGV2_INFO(75899, "Starting", "name"_attr = name);
    _processors[name].executor->start();
    LOGV2_INFO(75900, "Started", "name"_attr = name);
}

void StreamManager::startSample(std::string name) {
    stdx::lock_guard<Latch> lk(_mutex);

    auto it = _processors.find(name);
    uassert(ErrorCode::kTemporaryUserErrorCode,
            str::stream() << "streamProcessor does not exist: " << name,
            it != _processors.end());

    OutputSampler::Options options;
    options.maxDocsToSample = std::numeric_limits<int32_t>::max();
    options.maxBytesToSample = 50 * (1 << 20);  // 50MB
    auto sampler = std::make_unique<OutputSampler>(std::move(options));

    auto& processorInfo = it->second;
    processorInfo.executor->addOutputSampler(sampler.get());
    processorInfo.context->outputSamplers.emplace_back(std::move(sampler));
}

}  // namespace streams
