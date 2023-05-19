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
#include "streams/commands/stream_ops_gen.h"
#include "streams/exec/context.h"
#include "streams/exec/executor.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/log_dead_letter_queue.h"
#include "streams/exec/mongodb_dead_letter_queue.h"
#include "streams/exec/parser.h"
#include "streams/exec/sample_data_source_operator.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

namespace {

static const auto _decoration = ServiceContext::declareDecoration<std::unique_ptr<StreamManager>>();

std::unique_ptr<DeadLetterQueue> makeDLQ(
    const NamespaceString& nss,
    const stdx::unordered_map<std::string, mongo::Connection>& connections,
    const boost::optional<StartOptions>& startOptions,
    ServiceContext* svcCtx) {
    if (startOptions && startOptions->getDlq()) {
        auto connectionName = startOptions->getDlq()->getConnectionName().toString();

        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "DLQ with connectionName " << connectionName << " not found",
                connections.contains(connectionName));
        const auto& connection = connections.at(connectionName);
        uassert(ErrorCodes::InvalidOptions,
                "DLQ must be an Atlas collection",
                connection.getType() == mongo::ConnectionTypeEnum::Atlas);
        auto connectionOptions =
            AtlasConnectionOptions::parse(IDLParserContext("dlq"), connection.getOptions());
        return std::make_unique<MongoDBDeadLetterQueue>(
            nss,
            MongoDBDeadLetterQueue::Options{.svcCtx = svcCtx,
                                            .mongodbUri = connectionOptions.getUri().toString(),
                                            .database = startOptions->getDlq()->getDb().toString(),
                                            .collection =
                                                startOptions->getDlq()->getColl().toString()});
    } else {
        // TODO(SERVER-76564): Align with product on the right default DLQ behavior.
        return std::make_unique<LogDeadLetterQueue>(nss);
    }
}

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
    pruneStreamProcessors();
}

void StreamManager::pruneOutputSamplers() {
    stdx::lock_guard<Latch> lk(_mutex);
    for (auto& iter : _processors) {
        // Prune OutputSampler instances that haven't been polled by the client in over 5mins.
        auto smallestAllowedTimestamp =
            Date_t::now() - Seconds(_options.pruneInactiveSamplersAfterSeconds);
        auto& samplers = iter.second->outputSamplers;
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

void StreamManager::pruneStreamProcessors() {
    std::vector<std::string> erroSpNames;
    {
        stdx::lock_guard<Latch> lk(_mutex);
        for (auto& iter : _processors) {
            auto& processorInfo = iter.second;
            if (processorInfo->executorStatus.isOK()) {
                continue;
            }
            erroSpNames.push_back(iter.first);
        }
    }

    for (auto& name : erroSpNames) {
        stopStreamProcessor(name);
    }
}

void StreamManager::startStreamProcessor(std::string name,
                                         const std::vector<mongo::BSONObj>& pipeline,
                                         const std::vector<mongo::Connection>& connections,
                                         const boost::optional<mongo::StartOptions>& options) {
    auto executorFuture = startStreamProcessorInner(name, pipeline, connections, options);
    // Set onError continuation while _mutex is not held so that if the future is already fulfilled
    // and the continuation runs on the current thread itself it does not cause a deadlock.
    std::ignore = std::move(executorFuture).onError([this, name](Status status) {
        onExecutorError(name, std::move(status));
    });
}

mongo::Future<void> StreamManager::startStreamProcessorInner(
    std::string name,
    const std::vector<mongo::BSONObj>& pipeline,
    const std::vector<mongo::Connection>& connections,
    const boost::optional<mongo::StartOptions>& options) {
    stdx::lock_guard<Latch> lk(_mutex);

    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "streamProcessor name already exists: " << name,
            _processors.find(name) == _processors.end());

    stdx::unordered_map<std::string, mongo::Connection> connectionObjs;
    for (const auto& connection : connections) {
        uassert(ErrorCodes::InvalidOptions,
                "Connection names must be unique",
                !connectionObjs.contains(connection.getName().toString()));
        connectionObjs.emplace(std::make_pair(connection.getName(), connection));
    }

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
    context->dlq = makeDLQ(nss, connectionObjs, options, context->opCtx->getServiceContext());
    if (options && options->getEphemeral() && *options->getEphemeral()) {
        context->isEphemeral = true;
    }

    auto processorInfo = std::make_unique<StreamProcessorInfo>();
    processorInfo->context = std::move(context);

    Parser streamParser(processorInfo->context.get(), std::move(connectionObjs));

    LOGV2_INFO(75898, "Parsing", "name"_attr = name);
    processorInfo->operatorDag = streamParser.fromBson(pipeline);

    Executor::Options executorOptions;
    executorOptions.streamProcessorName = name;
    executorOptions.operatorDag = processorInfo->operatorDag.get();
    if (dynamic_cast<SampleDataSourceOperator*>(processorInfo->operatorDag->source())) {
        // If the customer is using a sample data source, sleep for 1 second between
        // every run.
        executorOptions.sourceNotIdleSleepDurationMs = 1000;
    }
    processorInfo->executor = std::make_unique<Executor>(std::move(executorOptions));
    processorInfo->startedAt = Date_t::now();
    processorInfo->streamStatus = StreamStatusEnum::Running;

    auto [it, inserted] = _processors.emplace(std::make_pair(name, std::move(processorInfo)));
    dassert(inserted);

    LOGV2_INFO(75899, "Starting stream processor", "name"_attr = name);
    auto executorFuture = it->second->executor->start();
    LOGV2_INFO(75900, "Started stream processor", "name"_attr = name);
    return executorFuture;
}

void StreamManager::stopStreamProcessor(std::string name) {
    std::unique_ptr<StreamProcessorInfo> processorInfo;
    {
        stdx::lock_guard<Latch> lk(_mutex);

        auto it = _processors.find(name);
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "streamProcessor does not exist: " << name,
                it != _processors.end());
        processorInfo = std::move(it->second);

        LOGV2_INFO(75901,
                   "Stopping stream processor",
                   "name"_attr = name,
                   "reason"_attr = processorInfo->executorStatus.reason());
        processorInfo->executor->stop();
        processorInfo->streamStatus = StreamStatusEnum::NotRunning;
        LOGV2_INFO(75902, "Stopped stream processor", "name"_attr = name);
        _processors.erase(it);
    }

    // Destroy processorInfo while the lock is not held.
    processorInfo.reset();
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
    processorInfo->executor->addOutputSampler(sampler.get());

    // Assign a unique cursor id to this sample request.
    int64_t cursorId = Date_t::now().toMillisSinceEpoch();
    if (processorInfo->lastCursorId == cursorId) {
        ++cursorId;
    }
    processorInfo->lastCursorId = cursorId;

    OutputSamplerInfo samplerInfo;
    samplerInfo.cursorId = cursorId;
    samplerInfo.outputSampler = std::move(sampler);
    processorInfo->outputSamplers.push_back(std::move(samplerInfo));
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
        processorInfo->outputSamplers.begin(),
        processorInfo->outputSamplers.end(),
        [cursorId](OutputSamplerInfo& samplerInfo) { return samplerInfo.cursorId == cursorId; });
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "cursor does not exist: " << cursorId,
            samplerIt != processorInfo->outputSamplers.end());

    OutputSample nextBatch;
    nextBatch.outputDocs = samplerIt->outputSampler->getNext(batchSize);
    if (samplerIt->outputSampler->doneSampling()) {
        nextBatch.doneSampling = true;
        // Since the OutputSampler is done sampling, remove it from
        // StreamProcessorInfo::outputSamplers. Any further getMoreFromSample() calls for this
        // cursor will fail.
        processorInfo->outputSamplers.erase(samplerIt);
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
    reply.setNs(processorInfo->context->expCtx->ns);
    reply.setName(name);
    reply.setStatus(processorInfo->streamStatus);
    reply.setScaleFactor(scale);

    auto stats = processorInfo->executor->getSummaryStats();
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
        replyItem.setNs(processorInfo->context->expCtx->ns);
        replyItem.setName(name);
        if (processorInfo->streamStatus == StreamStatusEnum::Running) {
            replyItem.setStartedAt(processorInfo->startedAt);
        }
        replyItem.setStatus(processorInfo->streamStatus);
        replyItem.setPipeline(processorInfo->operatorDag->bsonPipeline());
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

    processorInfo->executor->testOnlyInsertDocuments(std::move(docs));
}

void StreamManager::onExecutorError(std::string name, Status status) {
    invariant(!status.isOK());
    stdx::lock_guard<Latch> lk(_mutex);
    auto it = _processors.find(name);
    if (it == _processors.end()) {
        LOGV2_WARNING(75905,
                      "StreamProcessor does not exist",
                      "name"_attr = name,
                      "status"_attr = status.reason());
        return;
    }

    auto& processorInfo = it->second;
    invariant(processorInfo->executorStatus.isOK());
    processorInfo->executorStatus = std::move(status);
}

}  // namespace streams
