/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "mongo/util/scopeguard.h"
#include "streams/exec/checkpoint/local_disk_checkpoint_storage.h"
#include "streams/exec/operator_dag.h"
#include <chrono>
#include <exception>

#include "mongo/base/init.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/stdx/chrono.h"
#include "mongo/util/duration.h"
#include "mongo/util/exit.h"
#include "mongo/util/net/socket_utils.h"
#include "mongo/util/processinfo.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/exec/change_stream_source_operator.h"
#include "streams/exec/checkpoint_coordinator.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/config_gen.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/executor.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/merge_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/mongodb_checkpoint_storage.h"
#include "streams/exec/mongodb_dead_letter_queue.h"
#include "streams/exec/noop_dead_letter_queue.h"
#include "streams/exec/planner.h"
#include "streams/exec/sample_data_source_operator.h"
#include "streams/exec/stats_utils.h"
#include "streams/exec/stream_stats.h"
#include "streams/exec/tenant_feature_flags.h"
#include "streams/management/stream_manager.h"
#include <memory>

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

namespace {

static const auto _decoration = ServiceContext::declareDecoration<std::unique_ptr<StreamManager>>();

std::unique_ptr<DeadLetterQueue> createDLQ(Context* context,
                                           const boost::optional<StartOptions>& startOptions,
                                           ServiceContext* svcCtx) {
    if (startOptions && startOptions->getDlq()) {
        auto connectionName = startOptions->getDlq()->getConnectionName().toString();

        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "DLQ with connectionName " << connectionName << " not found",
                context->connections.contains(connectionName));
        const auto& connection = context->connections.at(connectionName);
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
        return std::make_unique<NoOpDeadLetterQueue>(context);
    }
}

bool isValidateOnlyRequest(const StartStreamProcessorCommand& request) {
    return request.getOptions() && request.getOptions()->getValidateOnly();
}

bool isCheckpointingAllowedForSource(OperatorDag* dag) {
    auto* source = dag->source();
    if (dynamic_cast<ChangeStreamSourceOperator*>(source)) {
        return true;
    } else if (dynamic_cast<KafkaConsumerOperator*>(source)) {
        return true;
    } else {
        return false;
    }
}

std::unique_ptr<OldCheckpointStorage> createCheckpointStorage(
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
        .svcCtx = svcCtx, .mongoClientOptions = std::move(mongoClientOptions)};
    return std::make_unique<MongoDBCheckpointStorage>(context, std::move(internalOptions));
}

using MetricKey = std::pair<MetricManager::LabelsVec, std::string>;

auto toMetricManagerLabels(const std::vector<MetricLabel>& labels) {
    MetricManager::LabelsVec metricManagerLabels;
    metricManagerLabels.reserve(labels.size());
    for (const auto& label : labels) {
        metricManagerLabels.push_back(
            std::make_pair(label.getKey().toString(), label.getValue().toString()));
    }
    return metricManagerLabels;
}

// Visitor class that is used to visit all the metrics in the MetricManager and construct a
// GetMetricsReply message.
class MetricsVisitor {
public:
    template <typename T>
    using MetricContainer = stdx::unordered_map<MetricKey, T, boost::hash<MetricKey>>;

    MetricsVisitor(MetricContainer<CounterMetricValue>* counterMap,
                   MetricContainer<GaugeMetricValue>* gaugeMap,
                   MetricContainer<HistogramMetricValue>* histogramMap)
        : _counterMap(counterMap), _gaugeMap(gaugeMap), _histogramMap(histogramMap) {}

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
        metricValue.setValue(counter->snapshotValue());
        metricValue.setLabels(toMetricLabels(labels));
        auto [it, inserted] =
            _counterMap->emplace(std::make_pair(toMetricManagerLabels(metricValue.getLabels()),
                                                metricValue.getName().toString()),
                                 metricValue);
        if (!inserted) {
            it->second.setValue(it->second.getValue() + metricValue.getValue());
        }
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

    void visit(Histogram* histogram,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        HistogramMetricValue metricValue;
        metricValue.setName(name);
        metricValue.setDescription(description);
        metricValue.setLabels(toMetricLabels(labels));

        auto buckets = histogram->snapshotValue();
        std::vector<HistogramBucket> bucketsReply;
        bucketsReply.reserve(buckets.size());
        for (const auto& bucket : buckets) {
            HistogramBucket bucketReply;
            bucketReply.setUpperBound(bucket.upper);
            bucketReply.setCount(bucket.count);
            bucketsReply.push_back(std::move(bucketReply));
        }
        metricValue.setBuckets(std::move(bucketsReply));

        auto [it, inserted] =
            _histogramMap->emplace(std::make_pair(toMetricManagerLabels(metricValue.getLabels()),
                                                  metricValue.getName().toString()),
                                   metricValue);
        if (!inserted) {
            auto& dst = it->second.getBuckets();
            const auto& src = metricValue.getBuckets();
            invariant(src.size() == dst.size());

            for (size_t i = 0; i < dst.size(); ++i) {
                auto& dstBucket = dst[i];
                const auto& srcBucket = src[i];
                invariant(srcBucket.getUpperBound() == dstBucket.getUpperBound());
                dstBucket.setCount(dstBucket.getCount() + srcBucket.getCount());
            }
        }
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
        metricValue.setValue(gauge->snapshotValue());
        metricValue.setLabels(toMetricLabels(labels));
        auto [it, inserted] =
            _gaugeMap->emplace(std::make_pair(toMetricManagerLabels(metricValue.getLabels()),
                                              metricValue.getName().toString()),
                               metricValue);
        if (!inserted) {
            it->second.setValue(it->second.getValue() + metricValue.getValue());
        }
    }

    MetricContainer<CounterMetricValue>* _counterMap{nullptr};
    MetricContainer<GaugeMetricValue>* _gaugeMap{nullptr};
    MetricContainer<HistogramMetricValue>* _histogramMap{nullptr};
};

template <typename M, typename V>
void mapToVec(const M& m, V& v) {
    v.reserve(m.size());
    for (auto [_, elem] : m) {
        v.push_back(elem);
    }
}

}  // namespace

StreamManager* getStreamManager(ServiceContext* svcCtx) {
    auto& streamManager = _decoration(svcCtx);
    static std::once_flag initOnce;
    std::call_once(initOnce, [&]() {
        dassert(!streamManager);
        StreamManager::Options options;

        int64_t memoryLimitBytes = mongo::streams::gStreamsMemoryLimitBytes;
        if (memoryLimitBytes > 0) {
            options.memoryLimitBytes = memoryLimitBytes;
        }

        streamManager = std::make_unique<StreamManager>(svcCtx, std::move(options));
        registerShutdownTask([&]() {
            LOGV2_INFO(75907, "Starting StreamManager shutdown");
            streamManager->shutdown();
        });
    });
    return streamManager.get();
}

void StreamManager::registerTenantMetrics(mongo::WithLock, const std::string& tenantId) {
    MetricManager::LabelsVec labels;
    labels.push_back(std::make_pair(kTenantIdLabelKey, tenantId));
    _memoryUsage = _metricManager->registerCallbackGauge(
        /* name */ "memory_usage_bytes",
        /* description */ "Current memory usage based on the internal memory usage tracking",
        labels,
        [this]() { return _memoryAggregator->getCurrentMemoryUsageBytes(); });

    for (size_t i = 0; i < idlEnumCount<StreamStatusEnum>; ++i) {
        labels.push_back(std::make_pair(kStatusLabelKey,
                                        std::string(StreamStatus_serializer(StreamStatusEnum(i)))));
        _numStreamProcessorsByStatusGauges[i] = _metricManager->registerGauge(
            "num_stream_processors_by_status",
            /* description */ "Active number of stream processors grouped by their current status",
            labels);
        labels.pop_back();
    }
}

StreamManager::StreamManager(ServiceContext* svcCtx, Options options)
    : _options(std::move(options)) {
    _metricManager = std::make_unique<MetricManager>();

    invariant(_options.memoryLimitBytes > 0);
    _memoryUsageMonitor = std::make_shared<KillAllMemoryUsageMonitor>(_options.memoryLimitBytes);
    _memoryAggregator = std::make_unique<ConcurrentMemoryAggregator>(_memoryUsageMonitor);
    _tenantFeatureFlags = std::make_shared<TenantFeatureFlags>();

    _streamProcessorStartRequestSuccessCounter = _metricManager->registerCounter(
        "stream_processor_requests_total",
        /* description */ "Total number of times start stream processor has succeeded",
        /* labels */ {{"request", "start"}, {"success", "true"}});
    _streamProcessorTotalStartLatencyCounter = _metricManager->registerCounter(
        "stream_processor_start_latency_ms_total",
        /* description */ "Total latency of all start stream processor commands",
        /* labels */ {});
    _streamProcessorStopRequestSuccessCounter = _metricManager->registerCounter(
        "stream_processor_requests_total",
        /* description */ "Total number of times stop stream processor has succeeded",
        /* labels */ {{"request", "stop"}, {"success", "true"}});
    _streamProcessorTotalStopLatencyCounter = _metricManager->registerCounter(
        "stream_processor_stop_latency_ms_total",
        /* description */ "Total latency of all stop stream processor commands",
        /* labels */ {});

    _streamProcessorActiveGauges[kStartCommand] =
        _metricManager->registerGauge("stream_processor_active_requests",
                                      /* description */ "Number of active start requests",
                                      /* labels */ {{"request", "start"}});
    _streamProcessorActiveGauges[kStopCommand] =
        _metricManager->registerGauge("stream_processor_active_requests",
                                      /* description */ "Number of active stop requests",
                                      /* labels */ {{"request", "stop"}});
    _streamProcessorActiveGauges[kListCommand] =
        _metricManager->registerGauge("stream_processor_active_requests",
                                      /* description */ "Number of active list requests",
                                      /* labels */ {{"request", "list"}});
    _streamProcessorActiveGauges[kStatsCommand] =
        _metricManager->registerGauge("stream_processor_active_requests",
                                      /* description */ "Number of active stats requests",
                                      /* labels */ {{"request", "stats"}});
    _streamProcessorActiveGauges[kSampleCommand] =
        _metricManager->registerGauge("stream_processor_active_requests",
                                      /* description */ "Number of active sample requests",
                                      /* labels */ {{"request", "sample"}});

    _streamProcessorFailedCounters[kStartCommand] = _metricManager->registerCounter(
        "stream_processor_requests_total",
        /* description */ "Total number of times start stream processor has failed",
        /* labels */ {{"request", "start"}, {"success", "false"}});
    _streamProcessorFailedCounters[kStopCommand] = _metricManager->registerCounter(
        "stream_processor_requests_total",
        /* description */ "Total number of times stop stream processor has failed",
        /* labels */ {{"request", "stop"}, {"success", "false"}});
    _streamProcessorFailedCounters[kListCommand] = _metricManager->registerCounter(
        "stream_processor_requests_total",
        /* description */ "Total number of times list stream processor has failed",
        /* labels */ {{"request", "list"}, {"success", "false"}});
    _streamProcessorFailedCounters[kSampleCommand] = _metricManager->registerCounter(
        "stream_processor_requests_total",
        /* description */ "Total number of times sample stream processor has failed",
        /* labels */ {{"request", "sample"}, {"success", "false"}});
    _streamProcessorFailedCounters[kStatsCommand] = _metricManager->registerCounter(
        "stream_processor_requests_total",
        /* description */ "Total number of times stats for a stream processor has failed",
        /* labels */ {{"request", "stats"}, {"success", "false"}});

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

std::pair<mongo::Status, mongo::Future<void>> StreamManager::waitForStartOrError(
    const std::string& name) {
    mongo::Future<void> executorFuture;
    {
        stdx::lock_guard<Latch> lk(_mutex);
        auto it = _processors.find(name);
        uassert(75985, "streamProcessor not found while starting", it != _processors.end());
        LOGV2_INFO(75880, "Starting stream processor", "context"_attr = it->second->context.get());
        executorFuture = it->second->executor->start();
        LOGV2_INFO(75981, "Started stream processor", "context"_attr = it->second->context.get());
    }

    auto getExecutorStartStatus =
        [this, name, &executorFuture]() -> boost::optional<mongo::Status> {
        stdx::lock_guard<Latch> lk(_mutex);

        auto it = _processors.find(name);
        if (it == _processors.end()) {
            static constexpr char reason[] =
                "streamProcessor was stopped while waiting for successful startup.";
            LOGV2_INFO(75941, reason, "name"_attr = name);
            return Status{ErrorCodes::Error(75932), std::string{reason}};
        }

        if (it->second->executor->isStarted()) {
            LOGV2_INFO(
                75940, "streamProcessor connected.", "context"_attr = it->second->context.get());
            return Status::OK();
        }

        if (executorFuture.isReady()) {
            auto status = executorFuture.getNoThrow();
            LOGV2_WARNING(
                75942,
                "Executor future returned early during start, likely due to a connection error.",
                "context"_attr = it->second->context.get(),
                "status"_attr = status);
            if (status.isOK()) {
                static constexpr char reason[] =
                    "Unexpected status after executor returned early during start.";
                LOGV2_ERROR(75943,
                            reason,
                            "context"_attr = it->second->context.get(),
                            "status"_attr = status);
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

StreamManager::StartResult StreamManager::startStreamProcessor(
    const mongo::StartStreamProcessorCommand& request) {
    Timer executionTimer;
    bool succeeded = false;
    boost::optional<int64_t> sampleCursorId;
    auto activeGauge = _streamProcessorActiveGauges[kStartCommand];
    ScopeGuard guard([&] {
        _streamProcessorTotalStartLatencyCounter->increment(executionTimer.millis());
        stdx::lock_guard<Latch> lk(_mutex);
        activeGauge->set(activeGauge->value() - 1);
        if (succeeded) {
            _streamProcessorStartRequestSuccessCounter->increment(1);
        } else {
            _streamProcessorFailedCounters[kStartCommand]->increment(1);
        }
    });

    std::string name = request.getName().toString();
    LOGV2_INFO(75883,
               "About to start stream processor",
               "correlationId"_attr = request.getCorrelationId(),
               "streamProcessorName"_attr = request.getName(),
               "streamProcessorId"_attr = request.getProcessorId(),
               "tenantId"_attr = request.getTenantId());
    bool shouldStopStreamProcessor = false;
    {
        stdx::lock_guard<Latch> lk(_mutex);
        activeGauge->set(activeGauge->value() + 1);
        uassert(75922, "StreamManager is shutting down, start cannot be called.", !_shutdown);

        auto processor = _processors.find(name);
        if (processor != _processors.end()) {
            // If the stream processor exists, ensure it's in an error state.
            uassert(ErrorCodes::InvalidOptions,
                    str::stream() << "streamProcessor name already exists: " << name,
                    processor->second->streamStatus == StreamStatusEnum::Error);

            LOGV2_INFO(8094501,
                       "StreamProcessor exists and is in an error state. Stopping it before "
                       "restarting it.",
                       "correlationId"_attr = request.getCorrelationId(),
                       "streamProcessorName"_attr = request.getName(),
                       "streamProcessorId"_attr = request.getProcessorId(),
                       "tenantId"_attr = request.getTenantId(),
                       "context"_attr = processor->second->context.get());

            shouldStopStreamProcessor = true;
        }
    }

    if (shouldStopStreamProcessor) {
        stopStreamProcessorByName(name, StopReason::ExternalStartRequestForFailedState);
    }

    {
        stdx::lock_guard<Latch> lk(_mutex);
        // TODO: Use processorId as the key in _processors map.
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "streamProcessor name already exists: " << name,
                _processors.find(name) == _processors.end());

        std::unique_ptr<StreamProcessorInfo> info = createStreamProcessorInfoLocked(request);
        if (isValidateOnlyRequest(request)) {
            // If this is a validateOnly request, return here without starting the streamProcessor.
            succeeded = true;
            return StartResult{boost::none};
        }

        if (!mongo::streams::gStreamsAllowMultiTenancy) {
            // Ensure all SPs running on this process belong to the same tenant ID.
            const auto& tenantId = info->context->tenantId;
            bool isAllSameTenantId =
                std::all_of(_processors.begin(), _processors.end(), [tenantId](const auto& e) {
                    return e.second->context->tenantId == tenantId;
                });
            uassert(
                8405900,
                "Not allowed to schedule a stream processor with a different tenant ID than the "
                "other running stream processors' tenant ID",
                isAllSameTenantId);
        }

        if (_processors.empty()) {
            // Recreate all Metric instances to use the new tenantId label.
            registerTenantMetrics(lk, info->context->tenantId);
        }

        // After we release the lock, no streamProcessor with the same name can be
        // inserted into the map.
        auto [it, inserted] = _processors.emplace(std::make_pair(name, std::move(info)));
        uassert(75982, "Failed to insert streamProcessor into _processors map", inserted);
    }

    if (request.getOptions() && request.getOptions()->getShouldStartSample()) {
        // If this stream processor is ephemeral, then start a sampling session before
        // starting the stream processor.
        StartStreamSampleCommand sampleRequest;
        sampleRequest.setCorrelationId(request.getCorrelationId());
        sampleRequest.setName(StringData(name));
        sampleCursorId = startSample(sampleRequest);
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
        succeeded = true;
    } else {
        stopStreamProcessorByName(name, StopReason::ErrorDuringStart);

        // Throw an error back to the client calling start.
        uasserted(status.code(), status.reason());
    }

    return StartResult{sampleCursorId};
}

std::unique_ptr<StreamManager::StreamProcessorInfo> StreamManager::createStreamProcessorInfoLocked(
    const mongo::StartStreamProcessorCommand& request) {
    ServiceContext* svcCtx = getGlobalServiceContext();
    const std::string name = request.getName().toString();

    auto context = std::make_unique<Context>();
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

    for (const auto& connection : request.getConnections()) {
        uassert(ErrorCodes::InvalidOptions,
                "Connection names must be unique",
                !context->connections.contains(connection.getName().toString()));
        auto ownedConnection = Connection(
            connection.getName().toString(), connection.getType(), connection.getOptions().copy());
        context->connections.emplace(
            std::make_pair(connection.getName(), std::move(ownedConnection)));
    }

    context->clientName = name + "-" + UUID::gen().toString();
    context->client = svcCtx->getService(ClusterRole::ShardServer)->makeClient(context->clientName);
    context->opCtx = svcCtx->makeOperationContext(context->client.get());

    // TODO(STREAMS-219)-PrivatePreview: We should make sure we're constructing the context
    // appropriately here
    context->expCtx = make_intrusive<ExpressionContext>(context->opCtx.get(),
                                                        std::unique_ptr<CollatorInterface>(nullptr),
                                                        NamespaceString(DatabaseName::kLocal));
    context->expCtx->allowDiskUse = false;
    context->memoryAggregator =
        _memoryAggregator->createChunkedMemoryAggregator(ChunkedMemoryAggregator::Options());

    const auto& options = request.getOptions();
    context->dlq = createDLQ(context.get(), options, context->opCtx->getServiceContext());
    if (options) {
        if (options->getEphemeral() && *options->getEphemeral()) {
            context->isEphemeral = true;
        }
    }

    if (options) {
        // Use metadata field only when the field name is not empty string.
        if (!options->getStreamMetaFieldName().empty()) {
            context->streamMetaFieldName = options->getStreamMetaFieldName().toString();
        }
    } else {
        // Set the metadata field name to the default option value if option isn't provided.
        context->streamMetaFieldName = StartOptions().getStreamMetaFieldName().toString();
    }

    auto processorInfo = std::make_unique<StreamProcessorInfo>();
    processorInfo->context = std::move(context);

    // TODO(SERVER-78464): When restoring from a checkpoint, we shouldn't be re-parsing
    // the user supplied BSON here to create the
    // DAG. Instead, we should re-parse from the exact plan stored in the checkpoint data.
    // We also need to validate somewhere that the startCommandBsonPipeline is still the
    // same.
    Planner::Options plannerOptions;
    if (options && options->getEnableUnnestedWindow()) {
        plannerOptions.unnestWindowPipeline = true;
    }
    Planner streamPlanner(processorInfo->context.get(), plannerOptions);
    LOGV2_INFO(75898, "Parsing", "context"_attr = processorInfo->context.get());
    processorInfo->operatorDag = streamPlanner.plan(request.getPipeline());

    // Configure checkpointing.
    bool checkpointEnabled = request.getOptions() && request.getOptions()->getCheckpointOptions() &&
        !isValidateOnlyRequest(request) && !processorInfo->context->isEphemeral &&
        isCheckpointingAllowedForSource(processorInfo->operatorDag.get());
    if (checkpointEnabled) {
        const auto& checkpointOptions = request.getOptions()->getCheckpointOptions();
        if (checkpointOptions->getLocalDisk()) {
            uassert(
                7712802,
                "If localDisk checkpointing is enabled, unnestedWindowPipeline should be enabled.",
                plannerOptions.unnestWindowPipeline);

            // The checkpoint write root directory for this streamProcessor is:
            //  /prefix/tenantId/streamProcessorId
            auto writeDir =
                std::filesystem::path{
                    checkpointOptions->getLocalDisk()->getWriteDirectory().toString()} /
                processorInfo->context->tenantId / processorInfo->context->streamProcessorId;
            std::filesystem::path restoreDir;
            if (checkpointOptions->getLocalDisk()->getRestoreDirectory()) {
                // Set the checkpoint restore directory to the path supplied from the Agent.
                // If set it should be a path like:
                //  /prefix/tenantId/streamProcessorId/checkpointId
                restoreDir = std::filesystem::path{
                    checkpointOptions->getLocalDisk()->getRestoreDirectory()->toString()};
            }
            processorInfo->context->checkpointStorage =
                std::make_unique<LocalDiskCheckpointStorage>(
                    LocalDiskCheckpointStorage::Options{.writeRootDir = writeDir,
                                                        .restoreRootDir = restoreDir,
                                                        .hostName = getHostNameCached(),
                                                        .userPipeline = request.getPipeline()},
                    processorInfo->context.get());
            // restoreCheckpointId will only be set if a restoreDir path is set.
            processorInfo->context->restoreCheckpointId =
                processorInfo->context->checkpointStorage->getRestoreCheckpointId();
        } else {
            processorInfo->context->oldCheckpointStorage =
                createCheckpointStorage(*request.getOptions()->getCheckpointOptions()->getStorage(),
                                        processorInfo->context.get(),
                                        svcCtx);
            processorInfo->context->restoreCheckpointId =
                processorInfo->context->oldCheckpointStorage->readLatestCheckpointId();
        }

        LOGV2_INFO(75910,
                   "Restore checkpoint ID",
                   "context"_attr = processorInfo->context.get(),
                   "checkpointId"_attr = processorInfo->context->restoreCheckpointId);
        if (processorInfo->context->restoreCheckpointId) {
            if (processorInfo->context->oldCheckpointStorage) {
                auto checkpointInfo =
                    processorInfo->context->oldCheckpointStorage->readCheckpointInfo(
                        *processorInfo->context->restoreCheckpointId);
                uassert(75913, "Expected checkpointInfo document", checkpointInfo);
                processorInfo->restoreCheckpointOperatorInfo = checkpointInfo->getOperatorInfo();
            } else {
                // Note: Here we call startCheckpointRestore so we can get the stats from the
                // checkpoint. The Executor will later call checkpointRestored in its background
                // thread once the operator dag has been fully restored.
                processorInfo->context->restoredCheckpointDescription =
                    processorInfo->context->checkpointStorage->startCheckpointRestore(
                        *processorInfo->context->restoreCheckpointId);
                processorInfo->restoreCheckpointOperatorInfo =
                    processorInfo->context->checkpointStorage->getRestoreCheckpointOperatorInfo();
            }
        }

        if (checkpointOptions->getDebugOnlyIntervalMs()) {
            // If provided, use the client supplied interval.
            processorInfo->context->checkpointInterval =
                stdx::chrono::milliseconds{*checkpointOptions->getDebugOnlyIntervalMs()};
        }

        processorInfo->checkpointCoordinator =
            std::make_unique<CheckpointCoordinator>(CheckpointCoordinator::Options{
                .processorId = processorInfo->context->streamProcessorId,
                .oldStorage = processorInfo->context->oldCheckpointStorage.get(),
                .writeFirstCheckpoint = !processorInfo->context->restoreCheckpointId,
                .checkpointIntervalMs = processorInfo->context->checkpointInterval,
                .restoreCheckpointOperatorInfo = processorInfo->restoreCheckpointOperatorInfo,
                .storage = processorInfo->context->checkpointStorage.get()});
    }

    // Create the Executor.
    Executor::Options executorOptions;
    executorOptions.operatorDag = processorInfo->operatorDag.get();
    executorOptions.checkpointCoordinator = processorInfo->checkpointCoordinator.get();
    executorOptions.connectTimeout = Seconds{60};
    if (dynamic_cast<SampleDataSourceOperator*>(processorInfo->operatorDag->source())) {
        // If the customer is using a sample data source, sleep for 1 second between
        // every run.
        executorOptions.sourceNotIdleSleepDurationMs = 1000;
    }
    executorOptions.tenantFeatureFlags = _tenantFeatureFlags;
    processorInfo->executor =
        std::make_unique<Executor>(processorInfo->context.get(), std::move(executorOptions));
    processorInfo->startedAt = Date_t::now();
    processorInfo->streamStatus = StreamStatusEnum::Running;
    return processorInfo;
}

void StreamManager::stopStreamProcessor(const mongo::StopStreamProcessorCommand& request) {
    LOGV2_INFO(8238704,
               "Stopping stream processor",
               "correlationId"_attr = request.getCorrelationId(),
               "streamProcessorName"_attr = request.getName());

    auto activeGauge = _streamProcessorActiveGauges[kStopCommand];
    {
        // TODO(SERVER-85554): Use Gauge::incBy() instead.
        stdx::lock_guard<Latch> lk(_mutex);
        activeGauge->set(activeGauge->value() + 1);
    }

    Timer executionTimer;
    bool succeeded = false;
    ScopeGuard guard([&] {
        _streamProcessorTotalStopLatencyCounter->increment(executionTimer.millis());
        stdx::lock_guard<Latch> lk(_mutex);
        activeGauge->set(activeGauge->value() - 1);
        if (succeeded) {
            _streamProcessorStopRequestSuccessCounter->increment(1);
        } else {
            _streamProcessorFailedCounters[kStopCommand]->increment(1);
        }
    });


    stopStreamProcessorByName(request.getName().toString(), StopReason::ExternalStopRequest);
    succeeded = true;
}

void StreamManager::stopStreamProcessorByName(std::string name, StopReason stopReason) {
    Executor* executor{nullptr};
    Context* context{nullptr};
    StreamStatusEnum status;
    {
        stdx::lock_guard<Latch> lk(_mutex);
        auto it = _processors.find(name);
        uassert(75908,
                str::stream() << "streamProcessor does not exist: " << name,
                it != _processors.end());
        uassert(75918,
                str::stream() << "streamProcessor is already being stopped: " << name,
                it->second->streamStatus != StreamStatusEnum::Stopping);
        it->second->streamStatus = StreamStatusEnum::Stopping;
        executor = it->second->executor.get();
        context = it->second->context.get();
        status = it->second->streamStatus;
        LOGV2_INFO(75911,
                   "Stopping stream processor",
                   "context"_attr = it->second->context.get(),
                   "reason"_attr = it->second->executorStatus.reason());
    }

    executor->stop(stopReason);
    LOGV2_INFO(75902,
               "Stopped stream processor",
               "context"_attr = context,
               "stopReason"_attr = stopReasonToString(stopReason),
               "status"_attr = StreamStatus_serializer(status));

    // Remove the streamProcessor from the map.
    std::unique_ptr<StreamProcessorInfo> processorInfo;
    {
        stdx::lock_guard<Latch> lk(_mutex);
        auto it = _processors.find(name);
        uassert(75909,
                str::stream() << "streamProcessor does not exist: " << name,
                it != _processors.end());
        uassert(75930,
                "streamProcessor expected to be in stopping state",
                it->second->streamStatus == StreamStatusEnum::Stopping);
        processorInfo = std::move(it->second);
        _processors.erase(it);
    }

    // Destroy processorInfo while the lock is not held.
    processorInfo.reset();

    // If all stream processors are being stopped because the memory limit has been exceeded, then
    // make sure to reset the `exceeded memory limit` signal after all the stream processors have
    // been stopped so that new stream processors can be scheduled on this process afterwards.
    if (_memoryUsageMonitor->hasExceededMemoryLimit() && _processors.empty()) {
        _memoryUsageMonitor->reset();
    }
}

int64_t StreamManager::startSample(const StartStreamSampleCommand& request) {
    LOGV2_INFO(8238702,
               "Starting to sample the stream processor",
               "correlationId"_attr = request.getCorrelationId(),
               "streamProcessorName"_attr = request.getName(),
               "limit"_attr = request.getLimit());
    bool succeeded = false;
    auto activeGauge = _streamProcessorActiveGauges[kSampleCommand];
    ScopeGuard guard([&] {
        stdx::lock_guard<Latch> lk(_mutex);
        activeGauge->set(activeGauge->value() - 1);
        if (!succeeded) {
            _streamProcessorFailedCounters[kSampleCommand]->increment(1);
        }
    });

    stdx::lock_guard<Latch> lk(_mutex);
    activeGauge->set(activeGauge->value() + 1);
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
    succeeded = true;
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

GetStatsReply StreamManager::getStats(const mongo::GetStatsCommand& request) {
    stdx::lock_guard<Latch> lk(_mutex);
    return getStats(lk, request, getProcessorInfo(lk, request.getName().toString()));
}

mongo::VerboseStatus StreamManager::getVerboseStatus(
    mongo::WithLock lock,
    const std::string& name,
    StreamManager::StreamProcessorInfo* processorInfo) {
    VerboseStatus status;
    GetStatsCommand statsRequest;
    statsRequest.setName(StringData(name));
    statsRequest.setVerbose(true);
    status.setStats(getStats(lock, statsRequest, processorInfo));
    status.setIsCheckpointingEnabled(bool(processorInfo->checkpointCoordinator));
    status.setRestoredCheckpoint(processorInfo->executor->getRestoredCheckpointDescription());
    status.setLastCommittedCheckpoint(
        processorInfo->executor->getLastCommittedCheckpointDescription());
    return status;
}

StreamManager::StreamProcessorInfo* StreamManager::getProcessorInfo(mongo::WithLock lock,
                                                                    const std::string& name) {
    auto it = _processors.find(name);
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "streamProcessor does not exist: " << name,
            it != _processors.end());
    return it->second.get();
}

GetStatsReply StreamManager::getStats(mongo::WithLock lock,
                                      const mongo::GetStatsCommand& request,
                                      StreamProcessorInfo* processorInfo) {
    int64_t scale = request.getScale();
    bool verbose = request.getVerbose();
    std::string name = request.getName().toString();
    bool succeeded = false;
    auto activeGauge = _streamProcessorActiveGauges[kStatsCommand];
    ScopeGuard guard([&] {
        activeGauge->set(activeGauge->value() - 1);
        if (!succeeded) {
            _streamProcessorFailedCounters[kStatsCommand]->increment(1);
        }
    });

    dassert(scale > 0);

    LOGV2_DEBUG(8238703,
                2,
                "Getting stats for the stream processor",
                "correlationId"_attr = request.getCorrelationId(),
                "streamProcessorName"_attr = name,
                "scale"_attr = scale,
                "verbose"_attr = verbose);

    GetStatsReply reply;
    reply.setName(name);
    reply.setProcessorId(processorInfo->context->streamProcessorId);
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
    reply.setDlqMessageCount(summaryStats.numDlqDocs);
    reply.setDlqMessageSize(summaryStats.numDlqBytes);
    reply.setStateSize(summaryStats.memoryUsageBytes);

    if (summaryStats.watermark >= 0) {
        reply.setWatermark(Date_t::fromMillisSinceEpoch(summaryStats.watermark));
    }

    if (verbose) {
        // If this stream processor is using a kafka source, include the kafka source's partition
        // states.
        auto kafkaConsumerPartitionStates =
            processorInfo->executor->getKafkaConsumerPartitionStates();
        if (!kafkaConsumerPartitionStates.empty()) {
            std::vector<mongo::KafkaConsumerPartitionState> partitionStatesReply;
            partitionStatesReply.reserve(kafkaConsumerPartitionStates.size());
            for (auto& state : kafkaConsumerPartitionStates) {
                mongo::KafkaConsumerPartitionState stateReply;
                stateReply.setPartition(state.partition);
                stateReply.setCurrentOffset(state.currentOffset);
                stateReply.setCheckpointOffset(state.checkpointOffset);
                partitionStatesReply.push_back(std::move(stateReply));
            }
            reply.setKafkaPartitions(std::move(partitionStatesReply));
        }

        auto changeStreamState = processorInfo->executor->getChangeStreamState();
        if (changeStreamState) {
            reply.setChangeStreamState(changeStreamState.get());
        }

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
                           s.numDlqBytes,
                           s.memoryUsageBytes,
                           s.maxMemoryUsageBytes,
                           mongo::duration_cast<Seconds>(s.totalExecutionTime)});
        }
        reply.setOperatorStats(std::move(out));
    }
    succeeded = true;
    return reply;
}

ListStreamProcessorsReply StreamManager::listStreamProcessors(
    const mongo::ListStreamProcessorsCommand& request) {
    LOGV2_DEBUG(8238701,
                2,
                "Listing all stream processors",
                "correlationId"_attr = request.getCorrelationId());
    bool succeeded = false;
    auto activeGauge = _streamProcessorActiveGauges[kListCommand];
    ScopeGuard guard([&] {
        stdx::lock_guard<Latch> lk(_mutex);
        activeGauge->set(activeGauge->value() - 1);
        if (!succeeded) {
            _streamProcessorFailedCounters[kListCommand]->increment(1);
        }
    });

    bool isVerbose = request.getVerbose();

    stdx::lock_guard<Latch> lk(_mutex);
    activeGauge->set(activeGauge->value() + 1);

    std::vector<mongo::ListStreamProcessorsReplyItem> streamProcessors;
    streamProcessors.reserve(_processors.size());
    for (auto& [name, processorInfo] : _processors) {
        ListStreamProcessorsReplyItem replyItem;
        replyItem.setNs(processorInfo->context->expCtx->ns);
        replyItem.setName(name);
        if (!processorInfo->context->tenantId.empty()) {
            replyItem.setTenantId(StringData(processorInfo->context->tenantId));
        }
        if (!processorInfo->context->streamProcessorId.empty()) {
            replyItem.setProcessorId(StringData(processorInfo->context->streamProcessorId));
        }
        replyItem.setStartedAt(processorInfo->startedAt);
        replyItem.setStatus(processorInfo->streamStatus);
        if (!processorInfo->executorStatus.isOK()) {
            replyItem.setError(StreamError{processorInfo->executorStatus.code(),
                                           processorInfo->executorStatus.reason(),
                                           isRetryableStatus(processorInfo->executorStatus)});
        }
        replyItem.setPipeline(processorInfo->operatorDag->bsonPipeline());

        if (isVerbose) {
            replyItem.setVerboseStatus(getVerboseStatus(lk, name, processorInfo.get()));
        }

        streamProcessors.push_back(std::move(replyItem));
    }

    ListStreamProcessorsReply reply;
    reply.setStreamProcessors(std::move(streamProcessors));
    succeeded = true;
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

using MetricKey = std::pair<MetricManager::LabelsVec, std::string>;

GetMetricsReply StreamManager::getMetrics() {
    GetMetricsReply reply;
    MetricsVisitor::MetricContainer<CounterMetricValue> counterMap;
    MetricsVisitor::MetricContainer<GaugeMetricValue> gaugeMap;
    MetricsVisitor::MetricContainer<HistogramMetricValue> histogramMap;
    std::array<int64_t, idlEnumCount<StreamStatusEnum>> numStreamProcessorsByStatus;
    numStreamProcessorsByStatus.fill(0);

    {
        stdx::lock_guard<Latch> lk(_mutex);
        for (const auto& [_, sp] : _processors) {
            MetricsVisitor metricsVisitor(&counterMap, &gaugeMap, &histogramMap);
            sp->executor->getMetricManager()->visitAllMetrics(&metricsVisitor);
            numStreamProcessorsByStatus[static_cast<int32_t>(sp->streamStatus)]++;
        }
    }

    // Update SP count by status before taking a snapshot.
    for (size_t i = 0; i < numStreamProcessorsByStatus.size(); ++i) {
        if (_numStreamProcessorsByStatusGauges[i]) {
            _numStreamProcessorsByStatusGauges[i]->set(numStreamProcessorsByStatus[i]);
        }
    }

    // to visit all metrics that are outside of executors.
    _metricManager->takeSnapshot();
    MetricsVisitor metricsVisitor(&counterMap, &gaugeMap, &histogramMap);
    _metricManager->visitAllMetrics(&metricsVisitor);
    std::vector<CounterMetricValue> counters;
    mapToVec(counterMap, counters);
    std::vector<GaugeMetricValue> gauges;
    mapToVec(gaugeMap, gauges);
    std::vector<HistogramMetricValue> histograms;
    mapToVec(histogramMap, histograms);
    reply.setCounters(std::move(counters));
    reply.setGauges(std::move(gauges));
    reply.setHistograms(std::move(histograms));
    return reply;
}

mongo::UpdateFeatureFlagsReply StreamManager::updateFeatureFlags(
    // TODO SERVER-86336 This path needs to be changed to support multi tenancy.
    const mongo::UpdateFeatureFlagsCommand& request) {
    mongo::UpdateFeatureFlagsReply reply;
    _tenantFeatureFlags->updateFeatureFlags(request.getFeatureFlags());
    stdx::lock_guard<Latch> lk(_mutex);
    for (auto& iter : _processors) {
        iter.second->executor->onFeatureFlagsUpdated();
    }
    return reply;
}

mongo::GetFeatureFlagsReply StreamManager::testOnlyGetFeatureFlags(
    const mongo::GetFeatureFlagsCommand& request) {
    if (request.getStreamProcessor()) {
        std::string name = request.getStreamProcessor()->toString();
        stdx::lock_guard<Latch> lk(_mutex);
        auto processor = _processors.find(name);
        mongo::GetFeatureFlagsReply reply;
        if (processor != _processors.end()) {
            reply.setFeatureFlags(processor->second->executor->testOnlyGetFeatureFlags());
        }
        return reply;
    } else {
        mongo::GetFeatureFlagsReply reply;
        reply.setFeatureFlags(_tenantFeatureFlags->testOnlyGetFeatureFlags());
        return reply;
    }
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
    invariant(processorInfo->streamStatus == StreamStatusEnum::Running ||
              processorInfo->streamStatus == StreamStatusEnum::Stopping);
    processorInfo->streamStatus = StreamStatusEnum::Error;
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
            stopStreamProcessorByName(processorName, StopReason::Shutdown);
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
