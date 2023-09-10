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
#include "mongo/util/duration.h"
#include "mongo/util/exit.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/exec/checkpoint_coordinator.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/context.h"
#include "streams/exec/executor.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/log_dead_letter_queue.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/mongodb_checkpoint_storage.h"
#include "streams/exec/mongodb_dead_letter_queue.h"
#include "streams/exec/parser.h"
#include "streams/exec/sample_data_source_operator.h"
#include "streams/exec/stats_utils.h"
#include "streams/exec/stream_stats.h"
#include <chrono>
#include <exception>

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

namespace {

static const auto _decoration = ServiceContext::declareDecoration<std::unique_ptr<StreamManager>>();

std::unique_ptr<DeadLetterQueue> createDLQ(
    Context* context,
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
        MongoCxxClientOptions options(connectionOptions);
        options.svcCtx = svcCtx;
        options.database = startOptions->getDlq()->getDb().toString();
        options.collection = startOptions->getDlq()->getColl().toString();
        return std::make_unique<MongoDBDeadLetterQueue>(context, std::move(options));
    } else {
        // TODO(SERVER-76564): Align with product on the right default DLQ behavior.
        return std::make_unique<LogDeadLetterQueue>(context);
    }
}

bool isValidateOnlyRequest(const StartStreamProcessorCommand& request) {
    return request.getOptions() && request.getOptions()->getValidateOnly();
}

std::unique_ptr<CheckpointStorage> createCheckpointStorage(
    const CheckpointStorageOptions& storageOptions, Context* context, ServiceContext* svcCtx) {
    uassert(ErrorCodes::InvalidOptions,
            "streamProcessorId and tenantId must be set if checkpointing is enabled",
            !context->tenantId.empty() && !context->streamProcessorId.empty());

    MongoCxxClientOptions mongoClientOptions;
    mongoClientOptions.svcCtx = svcCtx;
    mongoClientOptions.uri = storageOptions.getUri().toString();
    mongoClientOptions.database = storageOptions.getDb().toString();
    mongoClientOptions.collection = storageOptions.getColl().toString();
    if (storageOptions.getPemFile()) {
        mongoClientOptions.pemFile = storageOptions.getPemFile()->toString();
    }
    if (storageOptions.getCaFile()) {
        mongoClientOptions.caFile = storageOptions.getCaFile()->toString();
    }
    MongoDBCheckpointStorage::Options internalOptions{
        .tenantId = context->tenantId,
        .streamProcessorId = context->streamProcessorId,
        .svcCtx = svcCtx,
        .mongoClientOptions = std::move(mongoClientOptions)};
    return std::make_unique<MongoDBCheckpointStorage>(std::move(internalOptions));
}

// Visitor class that is used to visit all the metrics in the MetricManager and construct a
// GetMetricsReply message.
class MetricsVisitor {
public:
    void fillGetMetricsReply(GetMetricsReply* reply) && {
        dassert(reply);
        reply->setCounters(std::move(_counters));
        reply->setGauges(std::move(_gauges));
    }

    auto toMetricLabels(const MetricManager::LabelsVec& labels) {
        std::vector<MetricLabel> metricLabels;
        metricLabels.reserve(labels.size());
        for (const auto& label : labels) {
            MetricLabel mLabel;
            mLabel.setKey(label.first);
            mLabel.setValue(label.second);
            metricLabels.push_back(std::move(mLabel));
        }
        return metricLabels;
    }

    void visit(Counter* counter,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        CounterMetricValue metricValue;
        metricValue.setName(name);
        metricValue.setDescription(description);
        metricValue.setValue(counter->value());
        metricValue.setLabels(toMetricLabels(labels));
        _counters.push_back(std::move(metricValue));
    }

    void visit(Gauge* gauge,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        visitGauge(gauge, name, description, labels);
    }

    void visit(CallbackGauge* gauge,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        visitGauge(gauge, name, description, labels);
    }

private:
    template <typename GaugeType>
    void visitGauge(GaugeType* gauge,
                    const std::string& name,
                    const std::string& description,
                    const MetricManager::LabelsVec& labels) {
        GaugeMetricValue metricValue;
        metricValue.setName(name);
        metricValue.setDescription(description);
        metricValue.setValue(gauge->value());
        metricValue.setLabels(toMetricLabels(labels));
        _gauges.push_back(std::move(metricValue));
    }

    std::vector<CounterMetricValue> _counters;
    std::vector<GaugeMetricValue> _gauges;
};

}  // namespace

StreamManager* getStreamManager(ServiceContext* svcCtx) {
    auto& streamManager = _decoration(svcCtx);
    static std::once_flag initOnce;
    std::call_once(initOnce, [&]() {
        dassert(!streamManager);
        streamManager = std::make_unique<StreamManager>(svcCtx, StreamManager::Options{});
        registerShutdownTask([&]() {
            LOGV2_INFO(75907, "Starting StreamManager shutdown");
            streamManager->shutdown();
        });
    });
    return streamManager.get();
}

StreamManager::StreamManager(ServiceContext* svcCtx, Options options)
    : _options(std::move(options)) {
    _metricManager = std::make_unique<MetricManager>();
    _numStreamProcessorsGauge = _metricManager->registerCallbackGauge(
        "num_stream_processors",
        /* description */ "Active number of stream processors that are currently running",
        /*labels*/ {},
        [this]() {
            stdx::lock_guard<Latch> lk(_mutex);
            return _processors.size();
        });

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

std::pair<mongo::Status, mongo::Future<void>> StreamManager::waitForStartOrError(
    const std::string& name) {
    mongo::Future<void> executorFuture;
    {
        stdx::lock_guard<Latch> lk(_mutex);
        auto it = _processors.find(name);
        uassert(75985, "streamProcessor not found while starting", it != _processors.end());
        LOGV2_INFO(75880, "Starting stream processor", "name"_attr = name);
        executorFuture = it->second->executor->start();
        LOGV2_INFO(75981, "Started stream processor", "name"_attr = name);
    }

    auto getExecutorStartStatus =
        [this, name, &executorFuture]() -> boost::optional<mongo::Status> {
        stdx::lock_guard<Latch> lk(_mutex);

        auto it = _processors.find(name);
        if (it == _processors.end()) {
            constexpr auto* reason =
                "streamProcessor was stopped while waiting for succesful startup.";
            LOGV2_INFO(75941, reason, "name"_attr = name);
            return Status{ErrorCodes::Error(75932), std::string{reason}};
        }

        if (it->second->executor->isStarted()) {
            LOGV2_INFO(75940, "streamProcessor connected.", "name"_attr = name);
            return Status::OK();
        }

        if (executorFuture.isReady()) {
            auto status = executorFuture.getNoThrow();
            LOGV2_WARNING(
                75942,
                "Executor future returned early during start, likely due to a connection error.",
                "status"_attr = status);
            if (status.isOK()) {
                constexpr auto* reason =
                    "Unexpected status after executor returned early during start.";
                LOGV2_ERROR(75943, reason, "status"_attr = status);
                return Status{ErrorCodes::UnknownError, std::string{reason}};
            }
            return Status{status.code(), status.reason()};
        }

        return boost::none;
    };

    // Wait for the executor to succesfully start or report an error.
    boost::optional<Status> status = getExecutorStartStatus();
    while (!status) {
        sleepFor(_options.executorPollingIntervalMs);
        status = getExecutorStartStatus();
    }
    return std::make_pair(*status, std::move(executorFuture));
}

void StreamManager::startStreamProcessor(const mongo::StartStreamProcessorCommand& request) {
    std::string name = request.getName().toString();
    {
        stdx::lock_guard<Latch> lk(_mutex);
        uassert(75922, "StreamManager is shutting down, start cannot be called.", !_shutdown);

        // TODO: Use processorId as the key in _processors map.
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "streamProcessor name already exists: " << name,
                _processors.find(name) == _processors.end());

        std::unique_ptr<StreamProcessorInfo> info = createStreamProcessorInfoLocked(request);
        if (isValidateOnlyRequest(request)) {
            // If this is a validateOnly request, return here without starting the streamProcessor.
            return;
        }

        // After we release the lock, no streamProcessor with the same name can be
        // inserted into the map.
        auto [it, inserted] = _processors.emplace(std::make_pair(name, std::move(info)));
        uassert(75982, "Failed to insert streamProcessor into _processors map", inserted);
    }

    auto result = waitForStartOrError(name);
    Status status = std::get<0>(result);
    Future<void> executorFuture = std::move(std::get<1>(result));
    if (status.isOK()) {
        // Succesfully started the streamProcessor. We will return an OK to the client calling
        // the start command.
        // Set the onError continuation to call onExecutorError.
        std::ignore = std::move(executorFuture).onError([this, name = name](Status status) {
            onExecutorError(name, std::move(status));
        });
    } else {
        // The startup failed because it failed to connect or the client stopped it.
        std::unique_ptr<StreamProcessorInfo> processorInfo;
        {
            // Remove the streamProcessor from the map.
            stdx::lock_guard<Latch> lk(_mutex);
            auto it = _processors.find(name);
            if (it != _processors.end()) {
                // The streamProcessor may not exist in the map if it was stopped
                // in another thread.
                processorInfo = std::move(it->second);
                processorInfo->executor->stop();
                _processors.erase(it);
            }
        }

        // Destroy processorInfo while the lock is not held.
        processorInfo.reset();

        // Throw an error back to the client calling start.
        uasserted(status.code(), status.reason());
    }
}

std::unique_ptr<CheckpointCoordinator> StreamManager::createCheckpointCoordinator(
    const CheckpointOptions& checkpointOptions,
    StreamProcessorInfo* processorInfo,
    ServiceContext* svcCtx) {
    auto& context = processorInfo->context;
    invariant(context->checkpointStorage.get());
    CheckpointCoordinator::Options coordinatorOptions{
        .processorId = context->streamProcessorId,
        .storage = context->checkpointStorage.get(),
        .writeFirstCheckpoint = !processorInfo->context->restoreCheckpointId,
        .restoreCheckpointOperatorInfo = processorInfo->restoreCheckpointOperatorInfo};
    if (checkpointOptions.getDebugOnlyIntervalMs()) {
        // If provided, use the client supplied interval.
        coordinatorOptions.checkpointIntervalMs =
            stdx::chrono::milliseconds{*checkpointOptions.getDebugOnlyIntervalMs()};
    }
    return std::make_unique<CheckpointCoordinator>(std::move(coordinatorOptions));
}

std::unique_ptr<StreamManager::StreamProcessorInfo> StreamManager::createStreamProcessorInfoLocked(
    const mongo::StartStreamProcessorCommand& request) {
    ServiceContext* svcCtx = getGlobalServiceContext();
    const std::string name = request.getName().toString();
    stdx::unordered_map<std::string, mongo::Connection> connectionObjs;
    for (const auto& connection : request.getConnections()) {
        uassert(ErrorCodes::InvalidOptions,
                "Connection names must be unique",
                !connectionObjs.contains(connection.getName().toString()));
        connectionObjs.emplace(std::make_pair(connection.getName(), connection));
    }

    auto context = std::make_unique<Context>();
    context->metricManager = _metricManager.get();
    if (request.getTenantId()) {
        context->tenantId = request.getTenantId()->toString();
    }
    context->streamName = name;
    if (request.getProcessorId()) {
        context->streamProcessorId = request.getProcessorId()->toString();
    }
    uassert(ErrorCodes::InvalidOptions,
            "streamProcessorId and tenantId cannot contain '/' characters",
            context->tenantId.find('/') == std::string::npos &&
                context->streamProcessorId.find('/') == std::string::npos);

    context->clientName = name + "-" + UUID::gen().toString();
    context->client = svcCtx->makeClient(context->clientName);
    context->opCtx = svcCtx->makeOperationContext(context->client.get());
    // TODO(STREAMS-219)-PrivatePreview: We should make sure we're constructing the context
    // appropriately here
    context->expCtx = make_intrusive<ExpressionContext>(
        context->opCtx.get(), std::unique_ptr<CollatorInterface>(nullptr), NamespaceString());
    context->expCtx->allowDiskUse = false;

    const auto& options = request.getOptions();
    context->dlq =
        createDLQ(context.get(), connectionObjs, options, context->opCtx->getServiceContext());
    if (options && options->getEphemeral() && *options->getEphemeral()) {
        context->isEphemeral = true;
    }

    auto processorInfo = std::make_unique<StreamProcessorInfo>();
    processorInfo->context = std::move(context);

    // Configure checkpointing.
    bool checkpointEnabled = request.getOptions() && request.getOptions()->getCheckpointOptions() &&
        !isValidateOnlyRequest(request);
    if (checkpointEnabled) {
        processorInfo->context->checkpointStorage =
            createCheckpointStorage(request.getOptions()->getCheckpointOptions()->getStorage(),
                                    processorInfo->context.get(),
                                    svcCtx);
        processorInfo->context->restoreCheckpointId =
            processorInfo->context->checkpointStorage->readLatestCheckpointId();
        LOGV2_INFO(75910,
                   "Restore checkpoint ID",
                   "name"_attr = name,
                   "checkpointId"_attr = processorInfo->context->restoreCheckpointId);
        if (processorInfo->context->restoreCheckpointId) {
            auto checkpointInfo = processorInfo->context->checkpointStorage->readCheckpointInfo(
                *processorInfo->context->restoreCheckpointId);
            uassert(75913, "Expected checkpointInfo document", checkpointInfo);
            processorInfo->restoreCheckpointOperatorInfo = checkpointInfo->getOperatorInfo();
        }

        processorInfo->checkpointCoordinator = createCheckpointCoordinator(
            *options->getCheckpointOptions(), processorInfo.get(), svcCtx);
    }

    // Create the DAG by restoring from a checkpoint or parsing the user supplied pipeline.
    if (processorInfo->context->restoreCheckpointId) {
        LOGV2_INFO(75912,
                   "Restoring state from a checkpoint",
                   "name"_attr = name,
                   "checkpointId"_attr = *processorInfo->context->restoreCheckpointId);
        // TODO(SERVER-78464): We shouldn't be re-parsing the user supplied BSON here to create the
        // DAG. Instead, we should re-parse from the exact plan stored in the checkpoint data.
        // TODO(SERVER-78464): Validate somewhere that the startCommandBsonPipeline is still the
        // same.
        Parser streamParser(
            processorInfo->context.get(), Parser::Options{}, std::move(connectionObjs));
        processorInfo->operatorDag = streamParser.fromBson(request.getPipeline());
    } else {
        Parser streamParser(
            processorInfo->context.get(), Parser::Options{}, std::move(connectionObjs));
        LOGV2_INFO(75898, "Parsing", "name"_attr = name);
        processorInfo->operatorDag = streamParser.fromBson(request.getPipeline());
    }

    // Create the Executor.
    Executor::Options executorOptions;
    executorOptions.streamProcessorName = name;
    executorOptions.operatorDag = processorInfo->operatorDag.get();
    executorOptions.checkpointCoordinator = processorInfo->checkpointCoordinator.get();
    executorOptions.connectTimeout = Seconds{60};
    if (dynamic_cast<SampleDataSourceOperator*>(processorInfo->operatorDag->source())) {
        // If the customer is using a sample data source, sleep for 1 second between
        // every run.
        executorOptions.sourceNotIdleSleepDurationMs = 1000;
    }
    processorInfo->executor = std::make_unique<Executor>(std::move(executorOptions));
    processorInfo->startedAt = Date_t::now();
    processorInfo->streamStatus = StreamStatusEnum::Running;
    return processorInfo;
}

void StreamManager::stopStreamProcessor(std::string name) {
    std::unique_ptr<StreamProcessorInfo> processorInfo;
    {
        stdx::lock_guard<Latch> lk(_mutex);

        auto it = _processors.find(name);
        uassert(75908,
                str::stream() << "streamProcessor does not exist: " << name,
                it != _processors.end());
        processorInfo = std::move(it->second);

        LOGV2_INFO(75911,
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
    if (samplerIt->outputSampler->done()) {
        nextBatch.done = true;
        // Since the OutputSampler is done sampling, remove it from
        // StreamProcessorInfo::outputSamplers. Any further getMoreFromSample() calls for this
        // cursor will fail.
        processorInfo->outputSamplers.erase(samplerIt);
    }
    return nextBatch;
}

GetStatsReply StreamManager::getStats(std::string name, int64_t scale, bool verbose) {
    dassert(scale > 0);

    stdx::lock_guard<Latch> lk(_mutex);
    auto it = _processors.find(name);
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "streamProcessor does not exist: " << name,
            it != _processors.end());
    auto& processorInfo = it->second;

    GetStatsReply reply;
    reply.setName(name);
    reply.setStatus(processorInfo->streamStatus);
    reply.setScaleFactor(scale);

    auto operatorStats = processorInfo->executor->getOperatorStats();
    if (processorInfo->restoreCheckpointOperatorInfo) {
        std::vector<OperatorStats> checkpointStats;
        checkpointStats.reserve(processorInfo->restoreCheckpointOperatorInfo->size());
        for (auto& opInfo : *processorInfo->restoreCheckpointOperatorInfo) {
            checkpointStats.push_back(toOperatorStats(opInfo.getStats()));
        }
        operatorStats = combineAdditiveStats(operatorStats, checkpointStats);
    }
    auto summaryStats = computeStreamSummaryStats(operatorStats);

    reply.setInputMessageCount(summaryStats.numInputDocs);
    reply.setInputMessageSize(double(summaryStats.numInputBytes) / scale);
    reply.setOutputMessageCount(summaryStats.numOutputDocs);
    reply.setOutputMessageSize(double(summaryStats.numOutputBytes) / scale);
    reply.setStateSize(summaryStats.memoryUsageBytes);

    if (verbose) {
        std::vector<mongo::VerboseOperatorStats> out;
        out.reserve(operatorStats.size());
        for (size_t i = 0; i < operatorStats.size(); ++i) {
            auto& s = operatorStats[i];
            out.push_back({s.operatorName,
                           s.numInputDocs,
                           s.numInputBytes,
                           s.numOutputDocs,
                           s.numOutputBytes,
                           s.numDlqDocs,
                           s.memoryUsageBytes});
        }
        reply.setOperatorStats(std::move(out));
    }

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

GetMetricsReply StreamManager::getMetrics() {
    GetMetricsReply reply;
    MetricsVisitor visitor{};
    _metricManager->visitAllMetrics(&visitor);
    std::move(visitor).fillGetMetricsReply(&reply);
    return reply;
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

void StreamManager::shutdown() {
    {
        stdx::lock_guard<Latch> lock(_mutex);
        // After setting this bit, startStreamProcessor calls will fail.
        // Other methods can still be called.
        _shutdown = true;
    }
    stopAllStreamProcessors();
}

void StreamManager::stopAllStreamProcessors() {
    std::vector<std::string> streamProcessors;
    {
        stdx::lock_guard<Latch> lock(_mutex);
        streamProcessors.reserve(_processors.size());
        for (const auto& [name, sp] : _processors) {
            streamProcessors.push_back(name);
        }
    }

    LOGV2_INFO(75914, "Stopping all streamProcessors");
    for (const auto& processorName : streamProcessors) {
        try {
            stopStreamProcessor(processorName);
        } catch (const DBException& ex) {
            LOGV2_WARNING(75906,
                          "Failed to stop streamProcessor during stopAllStreamProcessors",
                          "name"_attr = processorName,
                          "errorCode"_attr = ex.code(),
                          "exception"_attr = ex.reason());
        }
    }
}

}  // namespace streams
