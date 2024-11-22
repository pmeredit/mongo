/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "mongo/util/assert_util.h"
#include <chrono>
#include <exception>
#include <memory>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/error_labels.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/stdx/chrono.h"
#include "mongo/util/duration.h"
#include "mongo/util/exit.h"
#include "mongo/util/net/socket_utils.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/str.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/exec/change_stream_source_operator.h"
#include "streams/exec/checkpoint/local_disk_checkpoint_storage.h"
#include "streams/exec/checkpoint_coordinator.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/concurrent_checkpoint_monitor.h"
#include "streams/exec/config_gen.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/executor.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/log_util.h"
#include "streams/exec/merge_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/mongodb_dead_letter_queue.h"
#include "streams/exec/noop_dead_letter_queue.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/planner.h"
#include "streams/exec/sample_data_source_operator.h"
#include "streams/exec/stats_utils.h"
#include "streams/exec/stream_processor_feature_flags.h"
#include "streams/exec/stream_stats.h"
#include "streams/exec/tenant_feature_flags.h"
#include "streams/exec/timeseries_emit_operator.h"
#include "streams/management/stream_manager.h"

using namespace mongo;

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

namespace {

static const auto _decoration = ServiceContext::declareDecoration<std::unique_ptr<StreamManager>>();

std::unique_ptr<DeadLetterQueue> createDLQ(Context* context,
                                           const StartOptions& startOptions,
                                           ServiceContext* svcCtx) {
    if (startOptions.getDlq()) {
        auto connectionName = startOptions.getDlq()->getConnectionName().toString();

        // The Agent supplies us with the connections, so this is an InternalError.
        uassert(mongo::ErrorCodes::InternalError,
                str::stream() << "DLQ with connectionName " << connectionName << " not found",
                context->connections.contains(connectionName));
        const auto& connection = context->connections.at(connectionName);
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "DLQ must be an Atlas collection",
                connection.getType() == mongo::ConnectionTypeEnum::Atlas);
        auto connectionOptions =
            AtlasConnectionOptions::parse(IDLParserContext("dlq"), connection.getOptions());
        MongoCxxClientOptions options(connectionOptions);
        options.svcCtx = svcCtx;
        options.database = startOptions.getDlq()->getDb().toString();
        options.collection = startOptions.getDlq()->getColl().toString();
        return std::make_unique<MongoDBDeadLetterQueue>(context, std::move(options));
    } else {
        return std::make_unique<NoOpDeadLetterQueue>(context);
    }
}

bool isValidateOnlyRequest(const StartStreamProcessorCommand& request) {
    return request.getOptions().getValidateOnly();
}

bool isParseOnlyRequest(const StartStreamProcessorCommand& request) {
    return request.getOptions().getParseOnly();
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

void validateOperatorsInCheckpoint(const std::vector<CheckpointOperatorInfo>& checkpointOperators,
                                   const OperatorDag::OperatorContainer& dagOperators) {
    auto numCheckpointOps = checkpointOperators.size();
    auto numOperatorDagOps = dagOperators.size();

    uassert(mongo::ErrorCodes::InternalError,
            fmt::format("Invalid checkpoint. Checkpoint has {} operators, OperatorDag has {}",
                        numCheckpointOps,
                        numOperatorDagOps),
            numCheckpointOps == numOperatorDagOps);

    for (size_t i = 0; i < checkpointOperators.size(); ++i) {
        const auto& checkpointOpName = checkpointOperators[i].getStats().getName();
        const auto& dagOpName = dagOperators[i]->getName();
        uassert(mongo::ErrorCodes::InternalError,
                fmt::format(
                    "Invalid checkpoint. Checkpoint operator {} name is {}, OperatorDag name is {}",
                    i,
                    checkpointOpName,
                    dagOpName),
                checkpointOpName == dagOpName);
    }
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
        // Counter and Gauge are thread-safe. We call value() here so that we get their
        // latest values even when Executor has not taken a snapshot of metrics in a while because
        // of a long-running runOnce() cycle.
        metricValue.setValue(counter->value());
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
        visitGaugeBase(gauge, name, description, labels);
    }

    void visit(IntGauge* gauge,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        visitGaugeBase(gauge, name, description, labels);
    }

    void visit(CallbackGauge* gauge,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        GaugeMetricValue metricValue;
        metricValue.setName(name);
        metricValue.setDescription(description);
        // CallbackGauge is not thread-safe. So we call snapshotValue() here instead of value().
        // This causes CallbackGauge values to always be a little stale compared to Counter/Gauge
        // values. This is not ideal, but also not easy to fix. We choose to live with it for now.
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
    void visitGaugeBase(GaugeType* gauge,
                        const std::string& name,
                        const std::string& description,
                        const MetricManager::LabelsVec& labels) {
        GaugeMetricValue metricValue;
        metricValue.setName(name);
        metricValue.setDescription(description);
        // Counter and Gauge are thread-safe. We call value() here so that we get their
        // latest values even when Executor has not taken a snapshot of metrics in a while because
        // of a long-running runOnce() cycle.
        metricValue.setValue(gauge->value());
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

    _streamProcessorActiveGauges[kStartCommand] =
        _metricManager->registerIntGauge("stream_processor_active_requests",
                                         /* description */ "Number of active start requests",
                                         /* labels */ {{"request", "start"}});
    _streamProcessorActiveGauges[kStopCommand] =
        _metricManager->registerIntGauge("stream_processor_active_requests",
                                         /* description */ "Number of active stop requests",
                                         /* labels */ {{"request", "stop"}});
    _streamProcessorActiveGauges[kListCommand] =
        _metricManager->registerIntGauge("stream_processor_active_requests",
                                         /* description */ "Number of active list requests",
                                         /* labels */ {{"request", "list"}});
    _streamProcessorActiveGauges[kStatsCommand] =
        _metricManager->registerIntGauge("stream_processor_active_requests",
                                         /* description */ "Number of active stats requests",
                                         /* labels */ {{"request", "stats"}});
    _streamProcessorActiveGauges[kSampleCommand] =
        _metricManager->registerIntGauge("stream_processor_active_requests",
                                         /* description */ "Number of active sample requests",
                                         /* labels */ {{"request", "sample"}});

    _streamProcessorRequestSuccessCounters[kStartCommand] = _metricManager->registerCounter(
        "stream_processor_requests_total",
        /* description */ "Total number of times start stream processor has succeeded",
        /* labels */ {{"request", "start"}, {"success", "true"}});
    _streamProcessorRequestSuccessCounters[kStopCommand] = _metricManager->registerCounter(
        "stream_processor_requests_total",
        /* description */ "Total number of times stop stream processor has succeeded",
        /* labels */ {{"request", "stop"}, {"success", "true"}});

    _streamProcessorRequestFailureCounters[kStartCommand] = _metricManager->registerCounter(
        "stream_processor_requests_total",
        /* description */ "Total number of times start stream processor has failed",
        /* labels */ {{"request", "start"}, {"success", "false"}});
    _streamProcessorRequestFailureCounters[kStopCommand] = _metricManager->registerCounter(
        "stream_processor_requests_total",
        /* description */ "Total number of times stop stream processor has failed",
        /* labels */ {{"request", "stop"}, {"success", "false"}});
    _streamProcessorRequestFailureCounters[kListCommand] = _metricManager->registerCounter(
        "stream_processor_requests_total",
        /* description */ "Total number of times list stream processor has failed",
        /* labels */ {{"request", "list"}, {"success", "false"}});
    _streamProcessorRequestFailureCounters[kSampleCommand] = _metricManager->registerCounter(
        "stream_processor_requests_total",
        /* description */ "Total number of times sample stream processor has failed",
        /* labels */ {{"request", "sample"}, {"success", "false"}});
    _streamProcessorRequestFailureCounters[kStatsCommand] = _metricManager->registerCounter(
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
    _containerStats.shutdown();

    stdx::lock_guard<stdx::mutex> lk(_mutex);
    tassert(8874300, "_tenantProcessors is not empty at shutdown", _tenantProcessors.empty());
}

void StreamManager::backgroundLoop() {
    pruneOutputSamplers();
}

void StreamManager::pruneOutputSamplers() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    for (auto& tenantIter : _tenantProcessors) {
        for (auto& iter : tenantIter.second->processors) {
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
}

void StreamManager::transitionToState(mongo::WithLock,
                                      StreamProcessorInfo* processorInfo,
                                      mongo::StreamStatusEnum newStatus) {
    switch (newStatus) {
        case StreamStatusEnum::Created:
            uasserted(mongo::ErrorCodes::InternalError,
                      str::stream() << "Unexpected state transition: "
                                    << StreamStatus_serializer(processorInfo->streamStatus)
                                    << " -> " << StreamStatus_serializer(newStatus));
            return;
        case StreamStatusEnum::Running:
            uassert(mongo::ErrorCodes::InternalError,
                    str::stream() << "Unexpected state transition: "
                                  << StreamStatus_serializer(processorInfo->streamStatus) << " -> "
                                  << StreamStatus_serializer(newStatus),
                    processorInfo->streamStatus == StreamStatusEnum::Created);
            processorInfo->streamStatus = newStatus;
            return;
        case StreamStatusEnum::Error:
            uassert(mongo::ErrorCodes::InternalError,
                    str::stream() << "Unexpected state transition: "
                                  << StreamStatus_serializer(processorInfo->streamStatus) << " -> "
                                  << StreamStatus_serializer(newStatus),
                    processorInfo->streamStatus == StreamStatusEnum::Running);
            processorInfo->streamStatus = newStatus;
            return;
        case StreamStatusEnum::Stopping:
            uassert(mongo::ErrorCodes::InternalError,
                    str::stream() << "Unexpected state transition: "
                                  << StreamStatus_serializer(processorInfo->streamStatus) << " -> "
                                  << StreamStatus_serializer(newStatus),
                    processorInfo->streamStatus == StreamStatusEnum::Running ||
                        processorInfo->streamStatus == StreamStatusEnum::Error);
            processorInfo->streamStatus = newStatus;
            return;
    }
}

StartStreamProcessorReply StreamManager::startStreamProcessor(
    const mongo::StartStreamProcessorCommand& request) {
    Timer executionTimer;
    auto activeGauge = _streamProcessorActiveGauges[kStartCommand];
    ScopeGuard guard([&] {
        activeGauge->incBy(-1);
        if (std::uncaught_exceptions()) {
            _streamProcessorRequestFailureCounters[kStartCommand]->increment(1);
        } else {
            _streamProcessorRequestSuccessCounters[kStartCommand]->increment(1);
        }
    });
    activeGauge->incBy(1);

    if (isParseOnlyRequest(request)) {
        auto connections = Planner::parseConnectionInfo(request.getPipeline());
        auto dlqOptions = request.getOptions().getDlq();
        if (dlqOptions) {
            connections.push_back(ParsedConnectionInfo{dlqOptions->getConnectionName().toString()});
        }
        StartStreamProcessorReply startReply;
        startReply.setConnections(std::move(connections));
        return startReply;
    }

    auto startReply = startStreamProcessorAsync(request);
    if (isValidateOnlyRequest(request)) {
        // If this is a validateOnly request, the streamProcessor is not started.
        return startReply;
    }

    std::string tenantId = request.getTenantId().toString();
    std::string name = request.getName().toString();
    auto processorId = request.getProcessorId();
    auto getExecutorStartStatus =
        [this, tenantId, name, processorId]() -> boost::optional<mongo::Status> {
        stdx::lock_guard<stdx::mutex> lk(_mutex);

        if (_shutdown) {
            static constexpr char reason[] = "start cannot be called during shutdown";
            LOGV2_INFO(9620101,
                       reason,
                       "streamProcessorName"_attr = name,
                       "streamProcessorId"_attr = processorId);
            return Status{ErrorCodes::StreamProcessorWorkerShuttingDown, std::string{reason}};
        }

        auto processorInfo = tryGetProcessorInfo(lk, tenantId, name);
        if (!processorInfo) {
            static constexpr char reason[] = "stream processor disappeared during start";
            LOGV2_INFO(75943,
                       reason,
                       "streamProcessorName"_attr = name,
                       "streamProcessorId"_attr = processorId);
            return Status{ErrorCodes::StreamProcessorDoesNotExist, std::string{reason}};
        }

        if (processorInfo->executor->isConnected()) {
            LOGV2_INFO(
                75940, "Stream processor connected", "context"_attr = processorInfo->context.get());
            return Status::OK();
        }

        if (processorInfo->executorStatus) {
            LOGV2_INFO(75942,
                       "Executor future returned early during start, likely due to a "
                       "connection error or an external stop request",
                       "context"_attr = processorInfo->context.get(),
                       "status"_attr = processorInfo->executorStatus);
            if (processorInfo->executorStatus->isOK()) {
                return Status{mongo::ErrorCodes::Error(75933),
                              "Executor future returned early during start"};
            } else {
                return processorInfo->executorStatus;
            }
        }
        return boost::none;
    };

    boost::optional<Status> status;
    Date_t deadline = Date_t::now() + request.getTimeout();
    {
        // Log state of all stream processors to help with analyzing rogue SP incidents
        ScopeGuard guard([&] {
            stdx::lock_guard<stdx::mutex> lk(_mutex);
            for (const auto& tenant : _tenantProcessors) {
                for (const auto& [name, processorInfo] : tenant.second->processors) {
                    LOGV2_INFO(
                        9420201,
                        "Stream processor state",
                        "correlationId"_attr = request.getCorrelationId(),
                        "context"_attr = processorInfo->context.get(),
                        "status"_attr = processorInfo->streamStatus,
                        "memoryTrackerBytes"_attr =
                            processorInfo->context->memoryAggregator->getCurrentMemoryUsageBytes());
                }
            }
        });

        // Wait for the executor to succesfully start or report an error.
        status = getExecutorStartStatus();
        while (!status) {
            sleepFor(Milliseconds(100));
            status = getExecutorStartStatus();
            uassert(75384, "Timeout while connecting", Date_t::now() <= deadline);
        }
    }

    tassert(9420202, "status should be set", status);
    if (!status->isOK()) {
        StopStreamProcessorCommand stopCommand;
        stopCommand.setTenantId(request.getTenantId());
        stopCommand.setName(request.getName());
        stopCommand.setProcessorId(request.getProcessorId());
        stopCommand.setCorrelationId(request.getCorrelationId());
        stopCommand.setTimeout(mongo::duration_cast<Seconds>(deadline - Date_t::now()));
        stopStreamProcessor(stopCommand, StopReason::ErrorDuringStart);

        // Throw an error back to the client calling start.
        uasserted(status->code(), status->reason());
    }
    return startReply;
}

boost::optional<std::string> StreamManager::getAssignedTenantId(mongo::WithLock) {
    if (mongo::streams::gStreamsAllowMultiTenancy || _tenantProcessors.empty()) {
        return boost::none;
    }
    return _tenantProcessors.begin()->second->tenantId;
}

void StreamManager::assertTenantIdIsValid(mongo::WithLock lk, mongo::StringData tenantId) {
    auto assignedTenantId = getAssignedTenantId(lk);
    uassert(mongo::ErrorCodes::InternalError,
            str::stream() << "Unexpected tenantId (" << tenantId << " vs " << *assignedTenantId
                          << ")",
            !assignedTenantId || *assignedTenantId == tenantId);
}

StartStreamProcessorReply StreamManager::startStreamProcessorAsync(
    const mongo::StartStreamProcessorCommand& request) {
    std::string tenantId = request.getTenantId().toString();
    std::string name = request.getName().toString();
    auto featureFlags =
        StreamProcessorFeatureFlags::parseFeatureFlags(request.getOptions().getFeatureFlags());
    LOGV2_INFO(75883,
               "About to start stream processor",
               "correlationId"_attr = request.getCorrelationId(),
               "streamProcessorName"_attr = request.getName(),
               "streamProcessorId"_attr = request.getProcessorId(),
               "tenantId"_attr = tenantId);

    bool shouldStopStreamProcessor = false;
    {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        uassert(ErrorCodes::StreamProcessorWorkerShuttingDown,
                "Worker is shutting down, start cannot be called",
                !_shutdown);

        auto processorInfo = tryGetProcessorInfo(lk, tenantId, name);
        if (processorInfo) {
            // If the stream processor exists, ensure it's in an error state.
            uassert(ErrorCodes::StreamProcessorAlreadyExists,
                    str::stream() << "stream processor name already exists: " << name,
                    processorInfo->streamStatus == StreamStatusEnum::Error);
            LOGV2_INFO(8094501,
                       "StreamProcessor exists and is in an error state. Stopping it before "
                       "restarting it",
                       "correlationId"_attr = request.getCorrelationId(),
                       "tenantId"_attr = tenantId,
                       "context"_attr = processorInfo->context.get());
            shouldStopStreamProcessor = true;
        }
    }

    if (shouldStopStreamProcessor) {
        StopStreamProcessorCommand stopCommand;
        stopCommand.setTenantId(request.getTenantId());
        stopCommand.setName(request.getName());
        stopCommand.setProcessorId(request.getProcessorId());
        stopCommand.setCorrelationId(request.getCorrelationId());
        stopCommand.setTimeout(request.getTimeout());
        stopStreamProcessor(stopCommand, StopReason::ExternalStartRequestForFailedState);
    }

    StartStreamProcessorReply startReply;
    boost::optional<int64_t> sampleCursorId;
    mongo::Future<void> executorFuture;
    {
        stdx::lock_guard<stdx::mutex> lk(_mutex);

        uassert(ErrorCodes::StreamProcessorAlreadyExists,
                str::stream() << "stream processor name already exists: " << name,
                !tryGetProcessorInfo(lk, tenantId, name));

        if (mongo::streams::gStreamsAllowMultiTenancy) {
            // We only allow multi-tenancy on a mongostream processor in some test scenarios.
            if (!_sourceBufferManager) {
                createSourceBufferManager(featureFlags, "*" /* tenantIdLabel */);
            }

            if (!_concurrentCheckpointController) {
                createConcurrentCheckpointController(featureFlags);
            }
        } else {
            // Ensure all SPs running on this process belong to the same tenant ID.
            assertTenantIdIsValid(lk, tenantId);

            if (_tenantProcessors.empty()) {
                // Recreate all Metric instances to use the new tenantId label.
                registerTenantMetrics(lk, tenantId);

                // Recreate _sourceBufferManager for the new tenantId.
                _sourceBufferManager.reset();
                createSourceBufferManager(featureFlags, tenantId);

                _concurrentCheckpointController.reset();
                createConcurrentCheckpointController(featureFlags);
            }
        }


        // Reset the `exceeded memory limit` signal after all the stream processors have been
        // stopped so that new stream processors can get scheduled on this worker.
        if (_memoryUsageMonitor->hasExceededMemoryLimit()) {
            bool resetMemoryUsageMonitor{true};
            for (auto& tenantIter : _tenantProcessors) {
                for (auto& iter : tenantIter.second->processors) {
                    if (iter.second->streamStatus == StreamStatusEnum::Running) {
                        resetMemoryUsageMonitor = false;
                    }
                }
            }

            if (resetMemoryUsageMonitor) {
                _memoryUsageMonitor->reset();
            }
        }

        std::unique_ptr<StreamProcessorInfo> info = createStreamProcessorInfo(lk, request);

        startReply.setOptimizedPipeline(info->operatorDag->optimizedPipeline());

        if (isValidateOnlyRequest(request)) {
            // If this is a validateOnly request, return here without starting the streamProcessor.
            return startReply;
        }

        // After we release the lock, no streamProcessor with the same name can be
        // inserted into the map.
        auto tenantInfo = getOrCreateTenantInfo(lk, tenantId);
        auto [it, inserted] = tenantInfo->processors.emplace(std::make_pair(name, std::move(info)));
        uassert(mongo::ErrorCodes::InternalError,
                "Failed to insert stream processor into processors map",
                inserted);
        auto& processorInfo = it->second;

        // TODO STREAMS-1320: update comment
        // We register this metric on the StreamManager's metric manager rather than the Executor's
        // to ensure accuracy if the processor gets stuck. This is necessary because we only take a
        // snapshot (takeSnapshot()) of the Executor's metrics within the runOnce() function, not
        // within the "streams_getMetrics" command, to avoid requiring the callback function
        // provided to the CallbackGauage to be thread-safe.
        processorInfo->durationSinceLastRunOnceGauge =
            _metricManager->registerGauge("duration_since_last_runonce",
                                          "Duration in milliseconds since the last runOnce()",
                                          getDefaultMetricLabels(processorInfo->context.get()),
                                          0);


        if (request.getOptions().getShouldStartSample()) {
            // If this stream processor is ephemeral, then start a sampling session before
            // starting the stream processor.
            StartStreamSampleCommand sampleRequest;
            sampleRequest.setCorrelationId(request.getCorrelationId());
            sampleRequest.setName(name);
            sampleCursorId = startSample(lk, sampleRequest, processorInfo.get());
        }

        LOGV2_INFO(
            75880, "Starting stream processor", "context"_attr = processorInfo->context.get());
        executorFuture = processorInfo->executor->start();
        LOGV2_INFO(
            75981, "Started stream processor", "context"_attr = processorInfo->context.get());
    }

    // Set the onError continuation to call onExecutorError.
    std::ignore =
        std::move(executorFuture)
            .then([this, tenantId, name]() { onExecutorShutdown(tenantId, name, Status::OK()); })
            .onError([this, tenantId, name](Status status) {
                onExecutorShutdown(tenantId, name, std::move(status));
            });
    startReply.setSampleCursorId(sampleCursorId);
    return startReply;
}

void StreamManager::createSourceBufferManager(const StreamProcessorFeatureFlags& featureFlags,
                                              const std::string& tenantId) {
    tassert(9395900, "Expected _sourceBufferManager not to be set", !_sourceBufferManager);
    SourceBufferManager::Options srcBufferOptions;
    srcBufferOptions.bufferTotalSize =
        *featureFlags.getFeatureFlagValue(FeatureFlags::kSourceBufferTotalSize).getInt();
    srcBufferOptions.bufferPreallocationFraction =
        *featureFlags.getFeatureFlagValue(FeatureFlags::kSourceBufferPreallocationFraction)
             .getDouble();
    srcBufferOptions.maxSourceBufferSize =
        *featureFlags.getFeatureFlagValue(FeatureFlags::kSourceBufferMaxSize).getInt();
    srcBufferOptions.minPageSize =
        *featureFlags.getFeatureFlagValue(FeatureFlags::kSourceBufferMinPageSize).getInt();
    srcBufferOptions.maxPageSize =
        *featureFlags.getFeatureFlagValue(FeatureFlags::kSourceBufferMaxPageSize).getInt();
    srcBufferOptions.metricManager = _metricManager.get();
    srcBufferOptions.metricLabels.push_back(std::make_pair(kTenantIdLabelKey, tenantId));
    _sourceBufferManager = std::make_shared<SourceBufferManager>(std::move(srcBufferOptions));
}

void StreamManager::createConcurrentCheckpointController(
    const StreamProcessorFeatureFlags& featureFlags) {
    tassert(9395901,
            "Expected _concurrentCheckpointController not to be set",
            !_concurrentCheckpointController);
    SourceBufferManager::Options srcBufferOptions;
    auto maxConcurrentCheckpoints =
        *featureFlags.getFeatureFlagValue(FeatureFlags::kMaxConcurrentCheckpoints).getInt();
    _concurrentCheckpointController =
        std::make_shared<ConcurrentCheckpointController>(maxConcurrentCheckpoints);
}

std::unique_ptr<StreamManager::StreamProcessorInfo> StreamManager::createStreamProcessorInfo(
    mongo::WithLock lk, const mongo::StartStreamProcessorCommand& request) {
    ServiceContext* svcCtx = getGlobalServiceContext();
    const std::string tenantId = request.getTenantId().toString();
    const std::string name = request.getName().toString();

    auto context = std::make_unique<Context>();
    context->tenantId = tenantId;
    context->streamName = name;
    context->streamProcessorId = request.getProcessorId().toString();

    if (request.getInstanceName()) {
        context->instanceName = request.getInstanceName()->toString();
    }

    context->featureFlags =
        StreamProcessorFeatureFlags::parseFeatureFlags(request.getOptions().getFeatureFlags());
    // The streams Agent sets the tenantID and stream processor ID, so this is an InternalError.
    uassert(mongo::ErrorCodes::InternalError,
            "streamProcessorId and tenantId cannot contain '/' characters",
            context->tenantId.find('/') == std::string::npos &&
                context->streamProcessorId.find('/') == std::string::npos);

    for (const auto& connection : request.getConnections()) {
        uassert(mongo::ErrorCodes::InternalError,
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
    context->expCtx = ExpressionContextBuilder{}
                          .opCtx(context->opCtx.get())
                          .allowDiskUse(false)
                          .ns(NamespaceString(DatabaseName::kLocal))
                          .build();
    context->memoryAggregator =
        _memoryAggregator->createChunkedMemoryAggregator(ChunkedMemoryAggregator::Options());
    context->sourceBufferManager = _sourceBufferManager;

    const auto& options = request.getOptions();
    context->dlq = createDLQ(context.get(), options, context->opCtx->getServiceContext());
    if (options.getEphemeral() && *options.getEphemeral()) {
        context->isEphemeral = true;
    }
    context->pipelineVersion = request.getPipelineVersion();

    context->concurrentCheckpointController = _concurrentCheckpointController;

    auto processorInfo = std::make_unique<StreamProcessorInfo>();
    processorInfo->context = std::move(context);

    auto executorMetricManager = std::make_unique<MetricManager>();

    bool checkpointEnabled =
        request.getOptions().getCheckpointOptions() && !processorInfo->context->isEphemeral;
    bool isModifyRequest = false;
    std::vector<BSONObj> executionPlan;
    if (checkpointEnabled) {
        const auto& checkpointOptions = request.getOptions().getCheckpointOptions();

        // The checkpoint write root directory for this streamProcessor is:
        //  /prefix/tenantId/streamProcessorId
        auto writeDir =
            std::filesystem::path{
                checkpointOptions->getLocalDisk().getWriteDirectory().toString()} /
            processorInfo->context->tenantId / processorInfo->context->streamProcessorId;
        std::filesystem::path restoreDir;
        if (checkpointOptions->getLocalDisk().getRestoreDirectory()) {
            // Set the checkpoint restore directory to the path supplied from the Agent.
            // If set it should be a path like:
            //  /prefix/tenantId/streamProcessorId/checkpointId
            restoreDir = std::filesystem::path{
                checkpointOptions->getLocalDisk().getRestoreDirectory()->toString()};
        }
        processorInfo->context->checkpointStorage = std::make_unique<LocalDiskCheckpointStorage>(
            LocalDiskCheckpointStorage::Options{.writeRootDir = writeDir,
                                                .restoreRootDir = restoreDir,
                                                .hostName = getHostNameCached(),
                                                .userPipeline = request.getPipeline()},
            processorInfo->context.get());
        processorInfo->context->checkpointStorage->registerMetrics(executorMetricManager.get());

        // restoreCheckpointId will only be set if a restoreDir path is set.
        processorInfo->context->restoreCheckpointId =
            processorInfo->context->checkpointStorage->getRestoreCheckpointId();

        if (processorInfo->context->restoreCheckpointId) {
            LOGV2_INFO(75910,
                       "Restore checkpoint ID",
                       "context"_attr = processorInfo->context.get(),
                       "checkpointId"_attr = processorInfo->context->restoreCheckpointId);

            // Note: Here we call startCheckpointRestore so we can get the stats from the
            // checkpoint. The Executor will later call checkpointRestored in its background
            // thread once the operator dag has been fully restored.
            processorInfo->context->restoredCheckpointInfo =
                processorInfo->context->checkpointStorage->startCheckpointRestore(
                    *processorInfo->context->restoreCheckpointId);
            tassert(9417503,
                    "Expected restored checkpoint fields to be set",
                    processorInfo->context->restoredCheckpointInfo &&
                        processorInfo->context->restoredCheckpointInfo->operatorInfo);
            if (!processorInfo->context->restoredCheckpointInfo->summaryStats) {
                // TODO(STREAMS-950): Remove this block once restoredCheckpointSummaryStats
                // is set in all checkpoints (all checkpoints are >= version 4).
                auto restoreStats =
                    toOperatorStats(*processorInfo->context->restoredCheckpointInfo->operatorInfo);
                processorInfo->context->restoredCheckpointInfo->summaryStats =
                    toSummaryStatsDoc(computeStreamSummaryStats(restoreStats));
            }

            // TODO(SERVER-92447): Remove this check.
            // If the feature flag is on, restore the stream processor using the execution plan
            // in the checkpoint.
            auto useExecutionPlanFromCheckpoint =
                processorInfo->context->featureFlags
                    ->getFeatureFlagValue(FeatureFlags::kUseExecutionPlanFromCheckpoint)
                    .getBool();
            if (useExecutionPlanFromCheckpoint && *useExecutionPlanFromCheckpoint) {
                if (processorInfo->context->restoredCheckpointInfo->executionPlan.empty()) {
                    LOGV2_WARNING(75908,
                                  "Execution plan not found in the checkpoint ",
                                  "context"_attr = processorInfo->context.get(),
                                  "checkpointId"_attr =
                                      processorInfo->context->restoreCheckpointId);
                } else {
                    LOGV2_INFO(75912,
                               "Using the Execution Plan from the checkpoint",
                               "context"_attr = processorInfo->context.get(),
                               "checkpointId"_attr = processorInfo->context->restoreCheckpointId,
                               "executionPlan"_attr = processorInfo->context->executionPlan);
                }
                executionPlan = processorInfo->context->restoredCheckpointInfo->executionPlan;
            }

            // Check if the pipelineVersion in the start request is different than the
            // pipelineVersion in the checkpoint. If so, this is an modify request.
            isModifyRequest = processorInfo->context->restoredCheckpointInfo->pipelineVersion !=
                request.getPipelineVersion();
            if (isValidateOnlyRequest(request)) {
                tassert(ErrorCodes::InternalError,
                        "validateOnly requests should only have a restore checkpoint if they "
                        "are validating a modify operation.",
                        isModifyRequest);
            }
            if (isModifyRequest) {
                LOGV2_INFO(9417500,
                           "Resuming a stream processor after an edit",
                           "context"_attr = processorInfo->context.get());
                // After an modify we don't use the executionPlan in the checkpoint.
                // This might change for "no-op" modify requests in SERVER-94910.
                executionPlan = {};
                // After an modify we don't use the operator level stats in the checkpoint.
                processorInfo->context->restoredCheckpointInfo->operatorInfo = boost::none;

                bool resumeFromCheckpointAfterModify =
                    request.getOptions().getResumeFromCheckpointAfterModify();
                if (isValidateOnlyRequest(request) || !resumeFromCheckpointAfterModify) {
                    // Mark the checkpoint as restored. For validateOnly and
                    // resumeFromCheckpointAfterModify=false requests, we won't use the restore
                    // checkpoint any further, i.e. operators won't restore their state from it.
                    processorInfo->context->checkpointStorage->checkpointRestored(
                        *processorInfo->context->restoreCheckpointId);
                }
                if (!resumeFromCheckpointAfterModify) {
                    // If resumeFromCheckpoint is false, unset the restoreCheckpointId so
                    // operators don't restore from their state in the restore checkpoint.
                    processorInfo->context->restoreCheckpointId = boost::none;
                }
            }
        }
    }

    Planner::Options plannerOptions;
    bool planUserPipeline = executionPlan.empty();
    if (planUserPipeline) {
        // Plan the OperatorDag using a user supplied pipeline.
        plannerOptions.planningUserPipeline = true;
        // During a customer's modify request, SPM sends a mongostream.validateOnly request with
        // a restore checkpoint. When resumeFromCheckpointAfterModify is true, we set
        // shouldValidateModifyRequest so the planner validates the modify is allowed.
        plannerOptions.shouldValidateModifyRequest = isValidateOnlyRequest(request) &&
            isModifyRequest && request.getOptions().getResumeFromCheckpointAfterModify();
    } else {
        // Plan the OperatorDag using an execution plan stored in checkpoints.
        plannerOptions.planningUserPipeline = false;
    }

    LOGV2_INFO(75898, "Parsing", "context"_attr = processorInfo->context.get());

    Planner streamPlanner(processorInfo->context.get(), std::move(plannerOptions));
    processorInfo->operatorDag =
        streamPlanner.plan(executionPlan.empty() ? request.getPipeline() : executionPlan);
    processorInfo->context->executionPlan = processorInfo->operatorDag->optimizedPipeline();

    if (checkpointEnabled) {
        auto isCheckpointSuportedForSource =
            isCheckpointingAllowedForSource(processorInfo->operatorDag.get());
        if (processorInfo->context->restoreCheckpointId) {
            tassert(8874301,
                    fmt::format("Checkpoint not supported for the source: {}",
                                processorInfo->operatorDag->source()->getName()),
                    isCheckpointSuportedForSource);
            tassert(8874305,
                    "Expected restoredCheckpointInfo to be set",
                    processorInfo->context->restoredCheckpointInfo);

            if (processorInfo->context->restoredCheckpointInfo->operatorInfo) {
                // TODO(SERVER-92447): Remove the validate operator check.
                // Validate the operators in the checkpoint match the OperatorDag we've created.
                validateOperatorsInCheckpoint(
                    *processorInfo->context->restoredCheckpointInfo->operatorInfo,
                    processorInfo->operatorDag->operators());
            }
        }

        // Start the checkpoint coordinator if checkpoint is supported for the given source.
        if (isCheckpointSuportedForSource) {
            const auto& checkpointOptions = request.getOptions().getCheckpointOptions();
            boost::optional<mongo::Milliseconds> fixedInterval;
            if (checkpointOptions->getDebugOnlyIntervalMs()) {
                // If provided, use the client supplied interval.
                fixedInterval = Milliseconds{*checkpointOptions->getDebugOnlyIntervalMs()};
            }

            auto minInterval =
                *processorInfo->context->featureFlags
                     ->getFeatureFlagValue(FeatureFlags::kCheckpointMinIntervalSeconds)
                     .getInt();
            auto maxInterval =
                *processorInfo->context->featureFlags
                     ->getFeatureFlagValue(FeatureFlags::kCheckpointMaxIntervalSeconds)
                     .getInt();
            auto byteSizeToUseMaxInterval =
                *processorInfo->context->featureFlags
                     ->getFeatureFlagValue(FeatureFlags::kCheckpointStateSizeToUseMaxIntervalBytes)
                     .getInt();
            processorInfo->checkpointCoordinator =
                std::make_unique<CheckpointCoordinator>(CheckpointCoordinator::Options{
                    .processorId = processorInfo->context->streamProcessorId,
                    .enableDataFlow = request.getOptions().getEnableDataFlow(),
                    .writeFirstCheckpoint = request.getOptions().getCheckpointOnStart() ||
                        !processorInfo->context->restoreCheckpointId,
                    .minInterval = Seconds{minInterval},
                    .maxInterval = Seconds{maxInterval},
                    .stateSizeToUseMaxInterval = byteSizeToUseMaxInterval,
                    .fixedInterval = fixedInterval,
                    .storage = processorInfo->context->checkpointStorage.get(),
                    .checkpointController = _concurrentCheckpointController,
                });
        } else {
            // Checkpoint is not supported for the given source, cleanup the CheckpointStorage
            // object.
            processorInfo->context->checkpointStorage.reset(nullptr);
        }
    }

    // Create the Executor.
    Executor::Options executorOptions;
    executorOptions.operatorDag = processorInfo->operatorDag.get();
    executorOptions.checkpointCoordinator = processorInfo->checkpointCoordinator.get();
    executorOptions.connectTimeout = Seconds{60};
    executorOptions.enableDataFlow = request.getOptions().getEnableDataFlow();
    executorOptions.metricManager = std::move(executorMetricManager);
    if (dynamic_cast<SampleDataSourceOperator*>(processorInfo->operatorDag->source())) {
        // If the customer is using a sample data source, sleep for 1 second between
        // every run.
        executorOptions.sourceNotIdleSleepDurationMs = 1000;
    }
    processorInfo->executor =
        std::make_unique<Executor>(processorInfo->context.get(), std::move(executorOptions));
    processorInfo->startedAt = Date_t::now();
    transitionToState(lk, processorInfo.get(), StreamStatusEnum::Running);
    return processorInfo;
}

void StreamManager::writeCheckpoint(const mongo::WriteStreamCheckpointCommand& request) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    assertTenantIdIsValid(lk, request.getTenantId());

    std::string tenantId = request.getTenantId().toString();
    std::string name = request.getName().toString();
    auto* info = getProcessorInfo(lk, tenantId, name);

    LOGV2_INFO(8017803,
               "Checkpointing stream processor",
               "context"_attr = info->context.get(),
               "correlationId"_attr = request.getCorrelationId());

    uassert(mongo::ErrorCodes::InternalError,
            str::stream() << "stream processor is being stopped: " << name,
            info->streamStatus != StreamStatusEnum::Stopping);
    info->executor.get()->writeCheckpoint(request.getForce());
}

StopStreamProcessorReply StreamManager::stopStreamProcessor(
    const mongo::StopStreamProcessorCommand& request) {
    LOGV2_INFO(8238704,
               "Stopping stream processor",
               "correlationId"_attr = request.getCorrelationId(),
               "streamProcessorId"_attr = request.getProcessorId(),
               "streamProcessorName"_attr = request.getName());

    Timer executionTimer;
    auto activeGauge = _streamProcessorActiveGauges[kStopCommand];
    ScopeGuard guard([&] {
        activeGauge->incBy(-1);
        if (std::uncaught_exceptions()) {
            _streamProcessorRequestFailureCounters[kStopCommand]->increment(1);
        } else {
            _streamProcessorRequestSuccessCounters[kStopCommand]->increment(1);
        }
    });
    activeGauge->incBy(1);

    return stopStreamProcessor(request, StopReason::ExternalStopRequest);
}

StopStreamProcessorReply StreamManager::stopStreamProcessor(
    const mongo::StopStreamProcessorCommand& request, StopReason stopReason) {
    stopStreamProcessorAsync(request, stopReason);

    std::string tenantId = request.getTenantId().toString();
    std::string name = request.getName().toString();
    auto processorId = request.getProcessorId();
    auto getExecutorStopStatus =
        [this, tenantId, name, processorId]() -> boost::optional<mongo::Status> {
        stdx::lock_guard<stdx::mutex> lk(_mutex);

        auto processorInfo = tryGetProcessorInfo(lk, tenantId, name);
        if (!processorInfo && _shutdown) {
            static constexpr char reason[] = "stop cannot be called during shutdown";
            LOGV2_INFO(9620102,
                       reason,
                       "streamProcessorName"_attr = name,
                       "streamProcessorId"_attr = processorId);
            return Status{ErrorCodes::StreamProcessorWorkerShuttingDown, std::string{reason}};
        }
        if (!processorInfo) {
            static constexpr char reason[] =
                "Stream processor disappeared while waiting for it to stop";
            LOGV2_INFO(75941,
                       reason,
                       "streamProcessorName"_attr = name,
                       "streamProcessorId"_attr = processorId);
            return Status{ErrorCodes::StreamProcessorDoesNotExist, std::string{reason}};
        }

        if (processorInfo->executorStatus) {
            LOGV2_INFO(
                75902, "Stopped stream processor", "context"_attr = processorInfo->context.get());
            return processorInfo->executorStatus;
        }
        return boost::none;
    };

    // Wait for the executor to succesfully stop or report an error.
    Date_t deadline = Date_t::now() + request.getTimeout();
    boost::optional<Status> status = getExecutorStopStatus();
    while (!status) {
        sleepFor(Milliseconds(100));
        status = getExecutorStopStatus();
        uassert(75383, "Timeout while stopping", Date_t::now() <= deadline);
    }

    // Get the final stats for the processor. The agent saves these so stats
    // can be retrieved while the processor is stopped.
    mongo::GetStatsCommand cmd;
    cmd.setVerbose(true);
    cmd.setTenantId(request.getTenantId());
    cmd.setProcessorId(request.getProcessorId());
    cmd.setName(request.getName());
    auto finalStats = getStats(std::move(cmd));

    // Remove the streamProcessor from the map.
    std::unique_ptr<StreamProcessorInfo> processorInfo;
    {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        auto tenantInfo = _tenantProcessors.find(tenantId);
        if (tenantInfo == _tenantProcessors.end()) {
            uassert(ErrorCodes::StreamProcessorWorkerShuttingDown,
                    str::stream() << "stop cannot be called during shutdown",
                    !_shutdown);
            uasserted(ErrorCodes::StreamProcessorDoesNotExist,
                      str::stream() << "stream processor does not exist: " << name);
        }

        auto it = tenantInfo->second->processors.find(name);
        uassert(ErrorCodes::StreamProcessorWorkerShuttingDown,
                str::stream() << "stop cannot be called during shutdown",
                !_shutdown || it != tenantInfo->second->processors.end());
        uassert(ErrorCodes::StreamProcessorDoesNotExist,
                str::stream() << "stream processor does not exist: " << name,
                it != tenantInfo->second->processors.end());
        uassert(mongo::ErrorCodes::InternalError,
                "Stream Processor expected to be in stopping state",
                it->second->streamStatus == StreamStatusEnum::Stopping);
        processorInfo = std::move(it->second);
        tenantInfo->second->processors.erase(it);
    }

    // Destroy processorInfo while the lock is not held.
    processorInfo.reset();

    {
        // Delete TenantInfo if we just stopped the last stream processor of the tenant.
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        auto tenantInfo = _tenantProcessors.find(tenantId);
        if (tenantInfo != _tenantProcessors.end() && tenantInfo->second->processors.empty()) {
            _tenantProcessors.erase(tenantId);
        }
    }

    return StopStreamProcessorReply{std::move(finalStats)};
}

void StreamManager::stopStreamProcessorAsync(const mongo::StopStreamProcessorCommand& request,
                                             StopReason stopReason) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    assertTenantIdIsValid(lk, request.getTenantId());

    std::string tenantId = request.getTenantId().toString();
    std::string name = request.getName().toString();
    auto processorInfo = tryGetProcessorInfo(lk, tenantId, name);
    uassert(ErrorCodes::StreamProcessorWorkerShuttingDown,
            str::stream() << "stop cannot be called during shutdown",
            processorInfo || !_shutdown);
    uassert(ErrorCodes::StreamProcessorDoesNotExist,
            str::stream() << "Stream processor does not exist: " << name,
            processorInfo);
    uassert(mongo::ErrorCodes::InternalError,
            str::stream() << "Unexpected tenantId (" << request.getTenantId() << " vs "
                          << processorInfo->context->tenantId << ")",
            request.getTenantId() == processorInfo->context->tenantId);
    if (processorInfo->streamStatus == StreamStatusEnum::Stopping) {
        const auto& executorStatus = processorInfo->executorStatus;
        LOGV2_INFO(9151101,
                   "Stream processor is already being stopped",
                   "context"_attr = processorInfo->context.get(),
                   "stopReason"_attr = stopReasonToString(stopReason),
                   "stopStatus"_attr = executorStatus ? executorStatus->reason() : "");
    } else {
        transitionToState(lk, processorInfo, StreamStatusEnum::Stopping);
        const auto& executorStatus = processorInfo->executorStatus;
        LOGV2_INFO(75911,
                   "Stopping stream processor",
                   "context"_attr = processorInfo->context.get(),
                   "stopReason"_attr = stopReasonToString(stopReason),
                   "stopStatus"_attr = executorStatus ? executorStatus->reason() : "");
        processorInfo->executor->stop(stopReason);
    }
}

int64_t StreamManager::startSample(const StartStreamSampleCommand& request) {
    LOGV2_INFO(8238702,
               "Starting to sample the stream processor",
               "correlationId"_attr = request.getCorrelationId(),
               "streamProcessorId"_attr = request.getProcessorId(),
               "streamProcessorName"_attr = request.getName(),
               "limit"_attr = request.getLimit());
    auto activeGauge = _streamProcessorActiveGauges[kSampleCommand];
    ScopeGuard guard([&] {
        activeGauge->incBy(-1);
        if (std::uncaught_exceptions()) {
            _streamProcessorRequestFailureCounters[kSampleCommand]->increment(1);
        }
    });
    activeGauge->incBy(1);

    stdx::lock_guard<stdx::mutex> lk(_mutex);

    assertTenantIdIsValid(lk, request.getTenantId());

    std::string tenantId = request.getTenantId().toString();
    std::string name = request.getName().toString();
    auto processorInfo = tryGetProcessorInfo(lk, tenantId, name);
    uassert(ErrorCodes::StreamProcessorDoesNotExist,
            str::stream() << "Stream processor does not exist: " << name,
            processorInfo);

    int64_t cursorId = startSample(lk, request, processorInfo);
    return cursorId;
}

int64_t StreamManager::startSample(mongo::WithLock,
                                   const StartStreamSampleCommand& request,
                                   StreamProcessorInfo* processorInfo) {
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

StreamManager::OutputSample StreamManager::getMoreFromSample(
    const mongo::GetMoreStreamSampleCommand& request) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    assertTenantIdIsValid(lk, request.getTenantId());

    std::string tenantId = request.getTenantId().toString();
    std::string name = request.getName().toString();
    auto processorInfo = tryGetProcessorInfo(lk, tenantId, name);
    uassert(ErrorCodes::StreamProcessorDoesNotExist,
            str::stream() << "Stream processor does not exist: " << name,
            processorInfo);

    int64_t cursorId = request.getCommandParameter();
    auto samplerIt = std::find_if(
        processorInfo->outputSamplers.begin(),
        processorInfo->outputSamplers.end(),
        [cursorId](OutputSamplerInfo& samplerInfo) { return samplerInfo.cursorId == cursorId; });
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "cursor does not exist: " << cursorId,
            samplerIt != processorInfo->outputSamplers.end());

    OutputSample nextBatch;
    nextBatch.outputDocs = samplerIt->outputSampler->getNext(request.getBatchSize());
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
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    assertTenantIdIsValid(lk, request.getTenantId());
    return getStats(
        lk,
        request,
        getProcessorInfo(lk, request.getTenantId().toString(), request.getName().toString()));
}

mongo::VerboseStatus StreamManager::getVerboseStatus(
    mongo::WithLock lock,
    const std::string& name,
    StreamManager::StreamProcessorInfo* processorInfo) {
    VerboseStatus status;
    GetStatsCommand statsRequest;
    statsRequest.setName(name);
    statsRequest.setVerbose(true);
    status.setStats(getStats(lock, statsRequest, processorInfo));
    status.setIsCheckpointingEnabled(bool(processorInfo->checkpointCoordinator));
    status.setRestoredCheckpoint(processorInfo->executor->getRestoredCheckpointDescription());
    status.setLastCommittedCheckpoint(
        processorInfo->executor->getLastCommittedCheckpointDescription());
    return status;
}

StreamManager::TenantInfo* StreamManager::getOrCreateTenantInfo(mongo::WithLock,
                                                                const std::string& tenantId) {
    auto tenantIter = _tenantProcessors.find(tenantId);
    if (tenantIter == _tenantProcessors.end()) {
        bool inserted{false};
        std::tie(tenantIter, inserted) =
            _tenantProcessors.emplace(tenantId, std::make_unique<TenantInfo>(tenantId));
        uassert(mongo::ErrorCodes::InternalError,
                "Failed to insert TenantInfo into _tenantProcessors map",
                inserted);
    }
    return tenantIter->second.get();
}

StreamManager::StreamProcessorInfo* StreamManager::tryGetProcessorInfo(mongo::WithLock lock,
                                                                       const std::string& tenantId,
                                                                       const std::string& name) {
    auto tenantIter = _tenantProcessors.find(tenantId);
    if (tenantIter == _tenantProcessors.end()) {
        return nullptr;
    }

    auto it = tenantIter->second->processors.find(name);
    if (it == tenantIter->second->processors.end()) {
        return nullptr;
    }
    return it->second.get();
}

StreamManager::StreamProcessorInfo* StreamManager::getProcessorInfo(mongo::WithLock lk,
                                                                    const std::string& tenantId,
                                                                    const std::string& name) {
    auto processorInfo = tryGetProcessorInfo(lk, tenantId, name);
    uassert(ErrorCodes::StreamProcessorDoesNotExist,
            str::stream() << "Stream processor does not exist: " << name,
            processorInfo);
    return processorInfo;
}

GetStatsReply StreamManager::getStats(mongo::WithLock lock,
                                      const mongo::GetStatsCommand& request,
                                      StreamProcessorInfo* processorInfo) {
    int64_t scale = request.getScale();
    bool verbose = request.getVerbose();
    bool isInternal = request.getIsInternal();
    std::string name = request.getName().toString();
    auto activeGauge = _streamProcessorActiveGauges[kStatsCommand];
    ScopeGuard guard([&] {
        activeGauge->incBy(-1);
        if (std::uncaught_exceptions()) {
            _streamProcessorRequestFailureCounters[kStatsCommand]->increment(1);
        }
    });
    activeGauge->incBy(1);

    dassert(scale > 0);

    LOGV2_DEBUG(8238703,
                2,
                "Getting stats for the stream processor",
                "correlationId"_attr = request.getCorrelationId(),
                "streamProcessorId"_attr = request.getProcessorId(),
                "streamProcessorName"_attr = name,
                "scale"_attr = scale,
                "verbose"_attr = verbose,
                "isInternal"_attr = isInternal);

    GetStatsReply reply;
    reply.setName(name);
    reply.setProcessorId(processorInfo->context->streamProcessorId);
    reply.setStatus(processorInfo->streamStatus);
    reply.setScaleFactor(scale);

    // Per-operator stats for this execution.
    auto currentOperatorStats = processorInfo->executor->getOperatorStats();
    // Per-operator stats for the lifetime of the processor (including stats in checkpoint).
    std::vector<OperatorStats> fullOperatorStats{currentOperatorStats};
    const auto& restoredCheckpointInfo = processorInfo->context->restoredCheckpointInfo;
    if (restoredCheckpointInfo && restoredCheckpointInfo->operatorInfo) {
        auto checkpointStats = toOperatorStats(*restoredCheckpointInfo->operatorInfo);
        if (currentOperatorStats.empty()) {
            // This can happen when the OperatorDag is still not fully initialized.
            currentOperatorStats = checkpointStats;
            fullOperatorStats = checkpointStats;
        } else {
            fullOperatorStats = combineAdditiveStats(currentOperatorStats, checkpointStats);
        }
    }

    StreamSummaryStats summaryStats = computeStreamSummaryStats(currentOperatorStats);
    if (restoredCheckpointInfo) {
        tassert(ErrorCodes::InternalError,
                "Expected summaryStats to be set",
                restoredCheckpointInfo->summaryStats);
        summaryStats += toSummaryStats(*restoredCheckpointInfo->summaryStats);
    }

    reply.setInputMessageCount(summaryStats.numInputDocs);
    reply.setInputMessageSize(double(summaryStats.numInputBytes) / scale);
    reply.setOutputMessageCount(summaryStats.numOutputDocs);
    reply.setOutputMessageSize(double(summaryStats.numOutputBytes) / scale);
    reply.setDlqMessageCount(summaryStats.numDlqDocs);
    reply.setDlqMessageSize(double(summaryStats.numDlqBytes) / scale);
    reply.setStateSize(double(summaryStats.memoryUsageBytes) / scale);
    reply.setMemoryTrackerBytes(double(_memoryAggregator->getCurrentMemoryUsageBytes()) / scale);

    if (summaryStats.watermark >= 0) {
        reply.setWatermark(Date_t::fromMillisSinceEpoch(summaryStats.watermark));
    }

    if (verbose) {
        // If this stream processor is using a kafka source, include the kafka source's partition
        // states and consumer group id.
        auto kafkaConsumerPartitionStates =
            processorInfo->executor->getKafkaConsumerPartitionStates();
        if (!kafkaConsumerPartitionStates.empty()) {
            boost::optional<int64_t> kafkaTotalOffsetLag{0};
            std::vector<mongo::KafkaConsumerPartitionState> partitionStatesReply;
            partitionStatesReply.reserve(kafkaConsumerPartitionStates.size());
            for (auto& state : kafkaConsumerPartitionStates) {
                mongo::KafkaConsumerPartitionState stateReply;
                stateReply.setTopic(state.topic);
                stateReply.setPartition(state.partition);
                stateReply.setCurrentOffset(state.currentOffset);
                stateReply.setCheckpointOffset(state.checkpointOffset);
                stateReply.setPartitionOffsetLag(state.partitionOffsetLag);
                stateReply.setIsIdle(state.isIdle);
                if (state.watermark >= 0) {
                    stateReply.setWatermark(Date_t::fromMillisSinceEpoch(state.watermark));
                }
                partitionStatesReply.push_back(std::move(stateReply));
                if (!state.partitionOffsetLag) {
                    kafkaTotalOffsetLag = boost::none;
                }
                if (kafkaTotalOffsetLag && state.partitionOffsetLag) {
                    *kafkaTotalOffsetLag += *state.partitionOffsetLag;
                }
            }
            reply.setKafkaPartitions(std::move(partitionStatesReply));
            reply.setKafkaTotalOffsetLag(kafkaTotalOffsetLag);
            reply.setKafkaConsumerGroup(processorInfo->context->kafkaConsumerGroup);
        }

        const auto& [changeStreamState, changeStreamLag] =
            processorInfo->executor->getChangeStreamState();
        if (changeStreamState) {
            reply.setChangeStreamState(changeStreamState.get());
            reply.setChangeStreamTimeDifferenceSecs(changeStreamLag);
        }

        std::vector<mongo::VerboseOperatorStats> out;
        out.reserve(fullOperatorStats.size());
        for (size_t i = 0; i < fullOperatorStats.size(); ++i) {
            auto& s = fullOperatorStats[i];
            VerboseOperatorStats stats{s.operatorName,
                                       s.numInputDocs,
                                       (double)s.numInputBytes / scale,
                                       s.numOutputDocs,
                                       (double)s.numOutputBytes / scale,
                                       s.numDlqDocs,
                                       (double)s.numDlqBytes / scale,
                                       (double)s.memoryUsageBytes / scale,
                                       (double)s.maxMemoryUsageBytes / scale,
                                       mongo::duration_cast<Seconds>(s.executionTime)};
            stats.setTimeSpentMillis(mongo::duration_cast<Milliseconds>(s.timeSpent));
            stats.setMinOpenWindowStartTime(s.minOpenWindowStartTime);
            stats.setMaxOpenWindowStartTime(s.maxOpenWindowStartTime);
            if (isInternal) {
                stats.setConnectionType(s.connectionType);
            }
            out.push_back(std::move(stats));
        }
        reply.setOperatorStats(std::move(out));
    }
    return reply;
}

ListStreamProcessorsReply StreamManager::listStreamProcessors(
    const mongo::ListStreamProcessorsCommand& request) {
    LOGV2_DEBUG(8238701,
                2,
                "Listing all stream processors",
                "correlationId"_attr = request.getCorrelationId());
    auto activeGauge = _streamProcessorActiveGauges[kListCommand];
    ScopeGuard guard([&] {
        activeGauge->incBy(-1);
        if (std::uncaught_exceptions()) {
            _streamProcessorRequestFailureCounters[kListCommand]->increment(1);
        }
    });
    activeGauge->incBy(1);

    stdx::lock_guard<stdx::mutex> lk(_mutex);

    if (request.getTenantId()) {
        assertTenantIdIsValid(lk, *request.getTenantId());
    }

    std::vector<mongo::ListStreamProcessorsReplyItem> streamProcessors;
    for (auto& tenantIter : _tenantProcessors) {
        if (streamProcessors.empty()) {
            streamProcessors.reserve(tenantIter.second->processors.size());
        }
        for (auto& [name, processorInfo] : tenantIter.second->processors) {
            if (request.getProcessorId() &&
                processorInfo->context->streamProcessorId != *request.getProcessorId()) {
                continue;
            }
            ListStreamProcessorsReplyItem replyItem;
            replyItem.setNs(processorInfo->context->expCtx->getNamespaceString());
            replyItem.setTenantId(processorInfo->context->tenantId);
            replyItem.setName(name);
            replyItem.setProcessorId(processorInfo->context->streamProcessorId);
            replyItem.setStartedAt(processorInfo->startedAt);
            replyItem.setStatus(processorInfo->streamStatus);
            if (processorInfo->executorStatus && !processorInfo->executorStatus->isOK()) {
                auto code = processorInfo->executorStatus->code();
                replyItem.setError(StreamError{code,
                                               processorInfo->executorStatus->reason(),
                                               isStreamProcessorRetryableError(code),
                                               isStreamProcessorUserError(code)});
            }
            replyItem.setPipeline(processorInfo->operatorDag->inputPipeline());

            if (request.getVerbose()) {
                replyItem.setVerboseStatus(getVerboseStatus(lk, name, processorInfo.get()));
            }

            streamProcessors.push_back(std::move(replyItem));
        }
    }

    ListStreamProcessorsReply reply;
    reply.setStreamProcessors(std::move(streamProcessors));
    return reply;
}

void StreamManager::testOnlyInsertDocuments(const mongo::TestOnlyInsertCommand& request) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    assertTenantIdIsValid(lk, request.getTenantId());

    std::string tenantId = request.getTenantId().toString();
    std::string name = request.getName().toString();
    auto* processorInfo = getProcessorInfo(lk, tenantId, name);

    // The incoming documents may not be owned. Since we need them to outlive this command
    // execution, get owned copies of them.
    std::vector<mongo::BSONObj> docs;
    for (const auto& doc : request.getDocuments()) {
        docs.push_back(doc.getOwned());
    }
    processorInfo->executor->testOnlyInsertDocuments(std::move(docs));
}

using MetricKey = std::pair<MetricManager::LabelsVec, std::string>;

GetMetricsReply StreamManager::getExternalMetrics() {
    GetMetricsReply reply;
    MetricsVisitor::MetricContainer<CounterMetricValue> counterMap;
    MetricsVisitor::MetricContainer<GaugeMetricValue> gaugeMap;
    MetricsVisitor::MetricContainer<HistogramMetricValue> histogramMap;

    // to visit all metrics that are outside of executors.
    _containerStats.getMetricManager()->takeSnapshot();
    MetricsVisitor metricsVisitor(&counterMap, &gaugeMap, &histogramMap);
    _containerStats.getMetricManager()->visitAllMetrics(&metricsVisitor);
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

GetMetricsReply StreamManager::getMetrics() {
    GetMetricsReply reply;
    MetricsVisitor::MetricContainer<CounterMetricValue> counterMap;
    MetricsVisitor::MetricContainer<GaugeMetricValue> gaugeMap;
    MetricsVisitor::MetricContainer<HistogramMetricValue> histogramMap;
    std::array<int64_t, idlEnumCount<StreamStatusEnum>> numStreamProcessorsByStatus;
    numStreamProcessorsByStatus.fill(0);

    {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        for (const auto& tenantIter : _tenantProcessors) {
            for (const auto& [_, sp] : tenantIter.second->processors) {
                MetricsVisitor metricsVisitor(&counterMap, &gaugeMap, &histogramMap);
                sp->executor->getMetricManager()->visitAllMetrics(&metricsVisitor);
                numStreamProcessorsByStatus[static_cast<int32_t>(sp->streamStatus)]++;

                sp->durationSinceLastRunOnceGauge->set(
                    sp->executor->durationSinceLastRunOnce().count());
            }
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
    const mongo::UpdateFeatureFlagsCommand& request) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    assertTenantIdIsValid(lk, request.getTenantId());

    auto tenantInfo = _tenantProcessors.find(request.getTenantId().toString());
    if (tenantInfo != _tenantProcessors.end()) {
        auto tenantFeatureFlags = std::make_shared<TenantFeatureFlags>(request.getFeatureFlags());
        for (auto& iter : tenantInfo->second->processors) {
            iter.second->executor->onFeatureFlagsUpdated(tenantFeatureFlags);
        }
        auto featureFlags = tenantFeatureFlags->getStreamProcessorFeatureFlags("");
        if (featureFlags.isOverridden(FeatureFlags::kMaxConcurrentCheckpoints)) {
            auto val =
                featureFlags.getFeatureFlagValue(FeatureFlags::kMaxConcurrentCheckpoints).getInt();
            if (val) {
                _concurrentCheckpointController->setMaxInprogressCheckpoints(
                    static_cast<int32_t>(*val));
            }
        }
    }
    return mongo::UpdateFeatureFlagsReply{};
}

mongo::GetFeatureFlagsReply StreamManager::testOnlyGetFeatureFlags(
    const mongo::GetFeatureFlagsCommand& request) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    assertTenantIdIsValid(lk, request.getTenantId());

    std::string tenantId = request.getTenantId().toString();
    std::string name = request.getName().toString();
    auto processor = getProcessorInfo(lk, tenantId, name);
    mongo::GetFeatureFlagsReply reply;
    reply.setFeatureFlags(processor->executor->testOnlyGetFeatureFlags());
    return reply;
}

void StreamManager::onExecutorShutdown(std::string tenantId, std::string name, Status status) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    auto processorInfo = tryGetProcessorInfo(lk, tenantId, name);
    if (!processorInfo) {
        LOGV2_WARNING(75905,
                      "StreamProcessor does not exist",
                      "streamProcessorName"_attr = name,
                      "status"_attr = status.reason());
        return;
    }

    invariant(processorInfo->streamStatus == StreamStatusEnum::Running ||
              processorInfo->streamStatus == StreamStatusEnum::Stopping);

    if (!processorInfo->executorStatus || processorInfo->executorStatus->isOK()) {
        processorInfo->executorStatus = std::move(status);
    }
    if (processorInfo->streamStatus == StreamStatusEnum::Running &&
        !processorInfo->executorStatus->isOK()) {
        transitionToState(lk, processorInfo, StreamStatusEnum::Error);
    }
}

void StreamManager::shutdown() {
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        // After setting this bit, startStreamProcessor calls will fail.
        // Other methods can still be called.
        _shutdown = true;
    }
    stopAllStreamProcessors();
}

void StreamManager::stopAllStreamProcessors() {
    std::vector<StopStreamProcessorCommand> stopCommands;
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        for (auto& tenantIter : _tenantProcessors) {
            if (stopCommands.empty()) {
                stopCommands.reserve(tenantIter.second->processors.size());
            }
            for (const auto& [name, sp] : tenantIter.second->processors) {
                StopStreamProcessorCommand stopCommand;
                stopCommand.setTenantId(sp->context->tenantId);
                stopCommand.setName(sp->context->streamName);
                stopCommand.setProcessorId(sp->context->streamProcessorId);
                stopCommands.push_back(std::move(stopCommand));
            }
        }
    }

    LOGV2_INFO(75914, "Stopping all stream processors");
    for (auto& stopCommand : stopCommands) {
        try {
            stopCommand.setTimeout(Seconds{3 * 60});
            stopStreamProcessor(stopCommand, StopReason::Shutdown);
        } catch (const DBException& ex) {
            LOGV2_WARNING(75906,
                          "Failed to stop stream processor during stopAllStreamProcessors",
                          "streamProcessorId"_attr = stopCommand.getProcessorId(),
                          "streamProcessorName"_attr = stopCommand.getName(),
                          "errorCode"_attr = ex.code(),
                          "exception"_attr = ex.reason());
        }
    }
}

mongo::SendEventReply StreamManager::sendEvent(const mongo::SendEventCommand& request) {
    uassert(mongo::ErrorCodes::InternalError,
            "Expected checkpointFlushed command",
            request.getCheckpointFlushedEvent());

    stdx::lock_guard<stdx::mutex> lk(_mutex);

    assertTenantIdIsValid(lk, request.getTenantId());

    // It's easier for the streams Agent to only supply a processor ID here (not name), so we lookup
    // the processor by ID.
    std::string tenantId = request.getTenantId().toString();
    auto tenantInfo = _tenantProcessors.find(tenantId);
    uassert(ErrorCodes::StreamProcessorDoesNotExist,
            fmt::format("streamProcessor with ID {} not found", request.getProcessorId()),
            tenantInfo != _tenantProcessors.end());

    auto it = std::find_if(tenantInfo->second->processors.begin(),
                           tenantInfo->second->processors.end(),
                           [&request](const auto& processor) {
                               return request.getProcessorId() ==
                                   processor.second->context->streamProcessorId;
                           });
    uassert(ErrorCodes::StreamProcessorDoesNotExist,
            fmt::format("streamProcessor with ID {} not found", request.getProcessorId()),
            it != tenantInfo->second->processors.end());
    uassert(mongo::ErrorCodes::InternalError,
            fmt::format("streamProcessor with ID {} does not have checkpoint storage",
                        request.getProcessorId()),
            it->second->context->checkpointStorage);

    // Hold the StreamManager mutex while calling checkpoint flush and getStats (to protect against
    // a concurrent stop).

    it->second->executor->onCheckpointFlushed(
        request.getCheckpointFlushedEvent()->getCheckpointId());

    mongo::GetStatsCommand cmd;
    cmd.setVerbose(true);
    cmd.setTenantId(request.getTenantId());
    cmd.setProcessorId(request.getProcessorId());
    cmd.setName(it->second->context->streamName);
    auto stats = getStats(lk, cmd, it->second.get());
    SendEventReply reply;
    reply.setCheckpointFlushedEventReply(CheckpointFlushedReply{std::move(stats)});
    return reply;
}

}  // namespace streams
