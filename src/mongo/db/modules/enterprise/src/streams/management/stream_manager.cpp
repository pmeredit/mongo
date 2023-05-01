/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/management/stream_manager.h"
#include "mongo/base/init.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/context.h"
#include "streams/exec/executor.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/log_dead_letter_queue.h"
#include "streams/exec/parser.h"
#include "streams/exec/stages_gen.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

namespace {

static const auto _decoration = ServiceContext::declareDecoration<std::unique_ptr<StreamManager>>();

}  // namespace

StreamManager* getStreamManager(ServiceContext* svcCtx) {
    auto& streamManager = _decoration(svcCtx);
    static std::once_flag initOnce;
    std::call_once(initOnce, [&]() {
        dassert(!streamManager);
        streamManager = std::make_unique<StreamManager>(svcCtx, StreamManager::Options{});
    });
    return streamManager.get();
}

StreamManager::StreamManager(ServiceContext* svcCtx, Options options)
    : _options(std::move(options)) {
    dassert(svcCtx);
    if (svcCtx->getPeriodicRunner()) {
        // Start the background job.
        _backgroundjob = svcCtx->getPeriodicRunner()->makeJob(PeriodicRunner::PeriodicJob{
            "StreamManagerBackgroundJob",
            [this](Client* client) { backgroundLoop(); },
            Seconds(_options.backgroundThreadPeriodSeconds),
            // TODO: Please revisit if this periodic job could be made killable.
            false /*isKillableByStepdown*/});
        _backgroundjob.start();
    }
}

StreamManager::~StreamManager() {
    if (_backgroundjob) {
        LOGV2_INFO(75903, "shutting down background job");
        _backgroundjob.stop();
        _backgroundjob.detach();
    }
}

void StreamManager::backgroundLoop() {
    pruneOutputSamplers();
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

    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "streamProcessor name already exists: " << name,
            _processors.find(name) == _processors.end());

    // TODO: Create a proper NamespaceString.
    NamespaceString nss{};
    auto context = std::make_unique<Context>();
    context->streamName = name;
    context->clientName = name + "-" + UUID::gen().toString();
    context->client = getGlobalServiceContext()->makeClient(context->clientName);
    context->opCtx = getGlobalServiceContext()->makeOperationContext(context->client.get());
    // TODO(STREAMS-219)-PrivatePreview: We should make sure we're constructing the context
    // appropriately here
    context->expCtx = make_intrusive<ExpressionContext>(
        context->opCtx.get(), std::unique_ptr<CollatorInterface>(nullptr), nss);
    context->expCtx->allowDiskUse = false;
    // TODO(STREAMS-219)-PrivatePreview: Considering exposing this as a parameter.
    // Or, set a parameter to dis-allow spilling.
    // We're using the same default as in run_aggregate.cpp.
    // This tempDir is used for spill to disk in $sort, $group, etc. stages
    // in window inner pipelines.
    context->expCtx->tempDir = storageGlobalParams.dbpath + "/_tmp";
    context->dlq = std::make_unique<LogDeadLetterQueue>(nss);

    StreamProcessorInfo processorInfo;
    processorInfo.context = std::move(context);

    Parser streamParser(processorInfo.context.get(), connections);

    LOGV2_INFO(75898, "Parsing", "name"_attr = name);
    processorInfo.operatorDag = streamParser.fromBson(pipeline);

    Executor::Options executorOptions;
    executorOptions.streamProcessorName = name;
    executorOptions.operatorDag = processorInfo.operatorDag.get();
    processorInfo.executor = std::make_unique<Executor>(std::move(executorOptions));
    processorInfo.startedAt = Date_t::now();
    processorInfo.streamStatus = StreamStatusEnum::Running;

    auto [it, inserted] = _processors.emplace(std::make_pair(name, std::move(processorInfo)));
    dassert(inserted);

    LOGV2_INFO(75899, "Starting", "name"_attr = name);
    it->second.executor->start();
    LOGV2_INFO(75900, "Started", "name"_attr = name);
}

void StreamManager::stopStreamProcessor(std::string name) {
    stdx::lock_guard<Latch> lk(_mutex);

    auto it = _processors.find(name);
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "streamProcessor does not exist: " << name,
            it != _processors.end());
    auto& processorInfo = it->second;

    LOGV2_INFO(75901, "Stopping", "name"_attr = name);
    processorInfo.executor->stop();
    processorInfo.streamStatus = StreamStatusEnum::NotRunning;
    LOGV2_INFO(75902, "Stopped", "name"_attr = name);

    _processors.erase(it);
}

int64_t StreamManager::startSample(const StartStreamSampleCommand& request) {
    stdx::lock_guard<Latch> lk(_mutex);

    std::string name = request.getName().toString();
    auto it = _processors.find(name);
    uassert(ErrorCodes::InvalidOptions,
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
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "streamProcessor does not exist: " << name,
            it != _processors.end());
    auto& processorInfo = it->second;

    auto samplerIt = std::find_if(
        processorInfo.outputSamplers.begin(),
        processorInfo.outputSamplers.end(),
        [cursorId](OutputSamplerInfo& samplerInfo) { return samplerInfo.cursorId == cursorId; });
    uassert(ErrorCodes::InvalidOptions,
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

GetStatsReply StreamManager::getStats(std::string name, int64_t scale) {
    dassert(scale > 0);

    stdx::lock_guard<Latch> lk(_mutex);
    auto it = _processors.find(name);
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "streamProcessor does not exist: " << name,
            it != _processors.end());
    auto& processorInfo = it->second;

    GetStatsReply reply;
    reply.setNs(processorInfo.context->expCtx->ns);
    reply.setName(name);
    reply.setStatus(processorInfo.streamStatus);
    reply.setScaleFactor(scale);

    auto stats = processorInfo.executor->getSummaryStats();
    reply.setInputDocs(stats.numInputDocs);
    reply.setInputBytes(double(stats.numInputBytes) / scale);
    reply.setOutputDocs(stats.numOutputDocs);
    reply.setOutputBytes(double(stats.numOutputBytes) / scale);
    return reply;
}

ListStreamProcessorsReply StreamManager::listStreamProcessors() {
    stdx::lock_guard<Latch> lk(_mutex);

    std::vector<mongo::ListStreamProcessorsReplyItem> streamProcessors;
    streamProcessors.reserve(_processors.size());
    for (auto& [name, processorInfo] : _processors) {
        ListStreamProcessorsReplyItem replyItem;
        replyItem.setNs(processorInfo.context->expCtx->ns);
        replyItem.setName(name);
        if (processorInfo.streamStatus == StreamStatusEnum::Running) {
            replyItem.setStartedAt(processorInfo.startedAt);
        }
        replyItem.setStatus(processorInfo.streamStatus);
        replyItem.setPipeline(processorInfo.operatorDag->bsonPipeline());
        streamProcessors.push_back(std::move(replyItem));
    }

    ListStreamProcessorsReply reply;
    reply.setStreamProcessors(std::move(streamProcessors));
    return reply;
}

void StreamManager::testOnlyInsertDocuments(std::string name, std::vector<mongo::BSONObj> docs) {
    stdx::lock_guard<Latch> lk(_mutex);

    auto it = _processors.find(name);
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "streamProcessor does not exist: " << name,
            it != _processors.end());
    auto& processorInfo = it->second;

    processorInfo.executor->testOnlyInsertDocuments(std::move(docs));
}

}  // namespace streams
