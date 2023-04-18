/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/management/stream_manager.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/operation_context.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/exec/connection_gen.h"
#include "streams/exec/context.h"
#include "streams/exec/executor.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/parser.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

StreamManager& StreamManager::get() {
    static std::unique_ptr<StreamManager> streamManager;
    static std::once_flag initOnce;
    std::call_once(initOnce, [&]() {
        streamManager = std::make_unique<StreamManager>(StreamManager::Options{});
    });
    return *streamManager;
}

StreamManager::StreamManager(Options options) : _options(std::move(options)) {
    // Start the background thread.
    _backgroundThread = stdx::thread([this] { backgroundLoop(); });
}

StreamManager::~StreamManager() {
    // Stop the background thread.
    bool joinThread{false};
    if (_backgroundThread.joinable()) {
        stdx::lock_guard<Latch> lock(_mutex);
        _shutdown = true;
        joinThread = true;
    }
    if (joinThread) {
        // Wait for the background thread to exit.
        _backgroundThread.join();
    }
}

void StreamManager::backgroundLoop() {
    while (true) {
        {
            stdx::lock_guard<Latch> lock(_mutex);
            if (_shutdown) {
                LOGV2_INFO(75903, "exiting backgroundLoop()");
                break;
            }
        }

        pruneOutputSamplers();

        stdx::this_thread::sleep_for(stdx::chrono::seconds(_options.backgroundThreadPeriodSeconds));
    }
}

void StreamManager::pruneOutputSamplers() {
    stdx::lock_guard<Latch> lk(_mutex);
    for (auto& iter : _processors) {
        // Prune OutputSampler instances that haven't been polled by the client in over 5mins.
        auto smallestAllowedTimestamp =
            Date_t::now() - Seconds(_options.pruneInactiveSamplersAfterSeconds);
        auto& samplers = iter.second.outputSamplers;
        for (auto samplerIt = samplers.begin(); samplerIt != samplers.end();) {
            if (samplerIt->outputSampler->getNextCallTimestamp() < smallestAllowedTimestamp) {
                samplerIt->outputSampler->cancel();
                samplerIt = samplers.erase(samplerIt);
            } else {
                ++samplerIt;
            }
        }
    }
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

    auto [it, inserted] = _processors.emplace(std::make_pair(name, std::move(processorInfo)));
    dassert(inserted);

    LOGV2_INFO(75899, "Starting", "name"_attr = name);
    it->second.executor->start();
    LOGV2_INFO(75900, "Started", "name"_attr = name);
}

void StreamManager::stopStreamProcessor(std::string name) {
    stdx::lock_guard<Latch> lk(_mutex);

    auto it = _processors.find(name);
    uassert(ErrorCode::kTemporaryUserErrorCode,
            str::stream() << "streamProcessor does not exist: " << name,
            it != _processors.end());

    LOGV2_INFO(75901, "Stopping", "name"_attr = name);
    it->second.executor->stop();
    LOGV2_INFO(75902, "Stopped", "name"_attr = name);

    _processors.erase(it);
}

int64_t StreamManager::startSample(const StartStreamSampleCommand& request) {
    stdx::lock_guard<Latch> lk(_mutex);

    std::string name = request.getName().toString();
    auto it = _processors.find(name);
    uassert(ErrorCode::kTemporaryUserErrorCode,
            str::stream() << "streamProcessor does not exist: " << name,
            it != _processors.end());
    auto& processorInfo = it->second;

    // Create an instance of OutputSampler for this sample request.
    OutputSampler::Options options;
    if (request.getLimit()) {
        options.maxDocsToSample = *request.getLimit();
    } else {
        options.maxDocsToSample = std::numeric_limits<int32_t>::max();
    }
    options.maxBytesToSample = 50 * (1 << 20);  // 50MB
    auto sampler = make_intrusive<OutputSampler>(std::move(options));
    processorInfo.executor->addOutputSampler(sampler.get());

    // Assign a unique cursor id to this sample request.
    int64_t cursorId = Date_t::now().toMillisSinceEpoch();
    if (processorInfo.lastCursorId == cursorId) {
        ++cursorId;
    }
    processorInfo.lastCursorId = cursorId;

    OutputSamplerInfo samplerInfo;
    samplerInfo.cursorId = cursorId;
    samplerInfo.outputSampler = std::move(sampler);
    processorInfo.outputSamplers.push_back(std::move(samplerInfo));
    return cursorId;
}

StreamManager::OutputSample StreamManager::getMoreFromSample(std::string name,
                                                             int64_t cursorId,
                                                             int64_t batchSize) {
    stdx::lock_guard<Latch> lk(_mutex);

    auto it = _processors.find(name);
    uassert(ErrorCode::kTemporaryUserErrorCode,
            str::stream() << "streamProcessor does not exist: " << name,
            it != _processors.end());
    auto& processorInfo = it->second;

    auto samplerIt = std::find_if(
        processorInfo.outputSamplers.begin(),
        processorInfo.outputSamplers.end(),
        [cursorId](OutputSamplerInfo& samplerInfo) { return samplerInfo.cursorId == cursorId; });
    uassert(ErrorCode::kTemporaryUserErrorCode,
            str::stream() << "cursor does not exist: " << cursorId,
            samplerIt != processorInfo.outputSamplers.end());

    OutputSample nextBatch;
    nextBatch.outputDocs = samplerIt->outputSampler->getNext(batchSize);
    if (samplerIt->outputSampler->doneSampling()) {
        nextBatch.doneSampling = true;
        // Since the OutputSampler is done sampling, remove it from
        // StreamProcessorInfo::outputSamplers. Any further getMoreFromSample() calls for this
        // cursor will fail.
        processorInfo.outputSamplers.erase(samplerIt);
    }
    return nextBatch;
}

void StreamManager::testOnlyInsertDocuments(std::string name, std::vector<mongo::BSONObj> docs) {
    stdx::lock_guard<Latch> lk(_mutex);

    auto it = _processors.find(name);
    uassert(ErrorCode::kTemporaryUserErrorCode,
            str::stream() << "streamProcessor does not exist: " << name,
            it != _processors.end());
    auto& processorInfo = it->second;

    processorInfo.executor->testOnlyInsertDocuments(std::move(docs));
}

}  // namespace streams
