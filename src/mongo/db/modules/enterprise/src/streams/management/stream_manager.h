/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#pragma once

#include <boost/optional.hpp>
#include <memory>

#include "mongo/platform/mutex.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/thread.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/concurrent_memory_aggregator.h"
#include "mongo/util/periodic_runner.h"
#include "mongo/util/processinfo.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/exec/checkpoint_coordinator.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"
#include "streams/exec/memory_usage_monitor.h"
#include "streams/exec/output_sampler.h"
#include "streams/exec/tenant_feature_flags.h"
#include "streams/util/metric_manager.h"

namespace mongo {
class Connection;
class StartOptions;
class ServiceContext;
template <typename>
class Future;
}  // namespace mongo

namespace streams {

class Executor;
class OperatorDag;

namespace {

// Fixed memory limit buffer space, the memory limit will be set to the process memory
// limit minus this buffer space.
static constexpr int64_t kMemoryLimitBufferSpaceBytes = 2LL * 1024 * 1024 * 1024;  // 2 GB

};  // namespace

/**
 * StreamManager is the entrypoint for all streamProcessor management operations.
 */
class StreamManager {
public:
    struct Options {
        // The period interval at which the background thread wakes up.
        int32_t backgroundThreadPeriodSeconds{60};
        // Prune inactive OutputSamplers after they have been inactive for this long.
        int32_t pruneInactiveSamplersAfterSeconds{5 * 60};
        // Sleep interval for the executor polling logic in StreamManager.
        mongo::Milliseconds executorPollingIntervalMs{100};
        // Max memory that the stream manager is allowed to use across all stream processors. Once
        // this limit is exceeded, all stream processors under this stream manager will be killed.
        int64_t memoryLimitBytes{static_cast<int64_t>(
            (mongo::ProcessInfo::getMemSizeMB() * 1024 * 1024) - kMemoryLimitBufferSpaceBytes)};
    };

    struct StartResult {
        // Only set when startRequest.options.shouldStartSample is true.
        boost::optional<int64_t> startSampleCursorId;
    };

    // Encapsulates a batch of sampled output records.
    struct OutputSample {
        std::vector<mongo::BSONObj> outputDocs;
        // Whether the sample request is fulfilled and there are no more output records to return
        // for this sample/cursor id.
        bool done{false};
    };

    StreamManager(mongo::ServiceContext* svcCtx, Options options);

    ~StreamManager();

    // Starts a new stream processor. Returns an optional sample cursor ID, which is only set if
    // the shouldStartSample is set in the start request.
    StartResult startStreamProcessor(const mongo::StartStreamProcessorCommand& request);

    // Stops the stream processor specified by request params.
    void stopStreamProcessor(const mongo::StopStreamProcessorCommand& request);

    // Causes stream processor to write a checkpoint
    void writeCheckpoint(const mongo::WriteStreamCheckpointCommand& request);

    // Starts a sample request for the given stream processor.
    // Returns the cursor id to use for this sample request in getMoreFromSample() calls.
    int64_t startSample(const mongo::StartStreamSampleCommand& request);

    // Returns the next batch of sampled output records for a sample created via startSample().
    // When OutputSample.done is true, the cursor is automatically closed, so the caller
    // should not make any more getMoreFromSample() calls for the a cursor.
    // Throws if the stream processor or the cursor is not found.
    OutputSample getMoreFromSample(const mongo::GetMoreStreamSampleCommand& request);

    // Returns stats for a stream processor.
    mongo::GetStatsReply getStats(const mongo::GetStatsCommand& request);

    // Returns the list of all stream processors.
    mongo::ListStreamProcessorsReply listStreamProcessors(
        const mongo::ListStreamProcessorsCommand& request);

    // Test-only method to insert documents into a stream processor.
    void testOnlyInsertDocuments(const mongo::TestOnlyInsertCommand& request);

    // Returns a GetMetricsReply message that contains current values of all the metrics
    // in the MetricManager.
    mongo::GetMetricsReply getMetrics();

    // Stops all the running streamProcessors and shuts down the StreamManager.
    // Called while processing a SIGTERM from Kubernetes in the Atlas Stream Processing service.
    void shutdown();

    // Used in send_event_command to notify mongostream about important events (like flushing a
    // checkpoint to remote storage.)
    void sendEvent(const mongo::SendEventCommand& request);

    // Updates feature flags for the tenant level.
    mongo::UpdateFeatureFlagsReply updateFeatureFlags(
        const mongo::UpdateFeatureFlagsCommand& request);

    // Gets feature flags for the tenant or stream processor (used by js tests.)
    mongo::GetFeatureFlagsReply testOnlyGetFeatureFlags(
        const mongo::GetFeatureFlagsCommand& request);

private:
    friend class StreamManagerTest;
    friend class CheckpointTest;

    // Encapsulates metadata for an OutputSampler.
    struct OutputSamplerInfo {
        int64_t cursorId{0};
        boost::intrusive_ptr<OutputSampler> outputSampler;
    };

    // Encapsulates state for a stream processor.
    struct StreamProcessorInfo {
        std::unique_ptr<Context> context;
        // TODO: Get startedAt time from SPM.
        mongo::Date_t startedAt;
        std::unique_ptr<OperatorDag> operatorDag;
        std::unique_ptr<Executor> executor;
        boost::optional<mongo::Status> executorStatus;
        // The list of active OutputSamplers created for the ongoing sample() requests.
        std::vector<OutputSamplerInfo> outputSamplers;
        // Last cursor id used for a sample request.
        int64_t lastCursorId{0};
        // Allowed State transitions:
        // Created -> Running
        // Running -> Error|Stopping
        // Error -> Stopping
        mongo::StreamStatusEnum streamStatus{mongo::StreamStatusEnum::Created};
        // CheckpointCoordinator for this streamProcessor.
        std::unique_ptr<CheckpointCoordinator> checkpointCoordinator;
        // Operator info in the restore checkpoint.
        boost::optional<std::vector<mongo::CheckpointOperatorInfo>> restoreCheckpointOperatorInfo;
    };

    enum Command { kStartCommand = 1, kStopCommand, kListCommand, kSampleCommand, kStatsCommand };

    // Recreates all Metric instances to use the given tenantId label.
    void registerTenantMetrics(mongo::WithLock, const std::string& tenantId);

    // Transitions StreamProcessorInfo.streamStatus to the given state.
    void transitionToState(mongo::WithLock,
                           StreamProcessorInfo* processorInfo,
                           mongo::StreamStatusEnum state);

    // Returns boost::none if gStreamsAllowMultiTenancy is true.
    // Otherwise, returns the assigned tenant id that all SPs need to correspond to.
    boost::optional<std::string> getAssignedTenantId(mongo::WithLock);

    // Asserts that requests for the given tenantId are permitted on this worker.
    void assertTenantIdIsValid(mongo::WithLock lk, mongo::StringData tenantId);

    // Caller must hold the _mutex.
    // Helper method used during startStreamProcessor. This parses the OperatorDag and creates
    // the Context and other things for the streamProcessor. This does not actually start
    // the streamProcessor or insert it into the _processors map. It is important that
    // this method does not write any data to the sources, sinks, DLQs, or checkpoint storage
    // for the streamProcessor.
    std::unique_ptr<StreamProcessorInfo> createStreamProcessorInfo(
        mongo::WithLock, const mongo::StartStreamProcessorCommand& request);

    // Helper method of the public startStreamProcessor() method that starts the stream processor
    // in an asynchronous manner.
    StartResult startStreamProcessorAsync(const mongo::StartStreamProcessorCommand& request);

    // Helper method of the public startSample() method.
    int64_t startSample(mongo::WithLock,
                        const mongo::StartStreamSampleCommand& request,
                        StreamProcessorInfo* processorInfo);

    void backgroundLoop();

    // Prunes OutputSampler instances that haven't been polled by the client in over 5mins.
    void pruneOutputSamplers();

    // Sets StreamProcessorInfo::executorStatus for the given executor.
    void onExecutorShutdown(std::string name, mongo::Status status);

    // Waits for the Executor to fully start the streamProcessor, or error out.
    std::pair<mongo::Status, mongo::Future<void>> waitForStartOrError(const std::string& name);

    // Returns stats for a stream processor.
    mongo::GetStatsReply getStats(mongo::WithLock,
                                  const mongo::GetStatsCommand& request,
                                  StreamManager::StreamProcessorInfo* processorInfo);

    // Stops a stream processor with the given name.
    void stopStreamProcessor(const mongo::StopStreamProcessorCommand& request,
                             StopReason stopReason);

    // Helper method of stopStreamProcessor() method that stops the stream processor in an
    // asynchronous manner.
    void stopStreamProcessorAsync(const mongo::StopStreamProcessorCommand& request,
                                  StopReason stopReason);

    // Stop all the running streamProcessors.
    void stopAllStreamProcessors();

    // Returns the verbose status of the stream processor.
    mongo::VerboseStatus getVerboseStatus(mongo::WithLock,
                                          const std::string& name,
                                          StreamManager::StreamProcessorInfo* processorInfo);

    // Returns the processor info for the given name.
    // uasserts if the stream processor does not exist.
    StreamProcessorInfo* getProcessorInfo(mongo::WithLock, const std::string& name);

    Options _options;
    std::unique_ptr<MetricManager> _metricManager;
    // The mutex that protects calls to startStreamProcessor.
    mongo::Mutex _mutex = MONGO_MAKE_LATCH("StreamManager::_mutex");
    // Memory aggregator that tracks memory usage across all active stream processors. This must be
    // placed before `_processors` to ensure that all child `ChunkedMemoryAggregator` instances are
    // destroyed before this parent `ConcurrentMemoryAggregator` is destroyed.
    std::unique_ptr<mongo::ConcurrentMemoryAggregator> _memoryAggregator;
    // The callback that `_memoryAggregator` invokes when the memory usage increases.
    std::shared_ptr<KillAllMemoryUsageMonitor> _memoryUsageMonitor;
    // The map of streamProcessors.
    mongo::stdx::unordered_map<std::string, std::unique_ptr<StreamProcessorInfo>> _processors;
    // Background job that performs any background operations like state pruning.
    mongo::PeriodicJobAnchor _backgroundjob;
    // Exports the number of stream processors.
    std::shared_ptr<CallbackGauge> _numStreamProcessorsGauge;
    // Exports the total count of startStreamProcessor.
    std::shared_ptr<Counter> _streamProcessorStartRequestSuccessCounter;
    // Exports the total latency of startStreamProcessor across all startStreamProcessor calls.
    std::shared_ptr<Counter> _streamProcessorTotalStartLatencyCounter;
    // Exports the total count of stopStreamProcessor.
    std::shared_ptr<Counter> _streamProcessorStopRequestSuccessCounter;
    // Exports the total latency of stopStreamProcessor across all stopStreamProcessor calls.
    std::shared_ptr<Counter> _streamProcessorTotalStopLatencyCounter;
    // Exports the current memory usage tracked by the internal memory usage tracker
    // `_memoryAggregator`.
    std::shared_ptr<CallbackGauge> _memoryUsage;
    // Exports the number of stream processors per stream status.
    std::array<std::shared_ptr<Gauge>, mongo::idlEnumCount<mongo::StreamStatusEnum>>
        _numStreamProcessorsByStatusGauges;
    mongo::stdx::unordered_map<Command, std::shared_ptr<IntGauge>> _streamProcessorActiveGauges;
    mongo::stdx::unordered_map<Command, std::shared_ptr<Counter>> _streamProcessorFailedCounters;

    // Set to true when stopAll is called. When true the client can't call startStreamProcessor.
    bool _shutdown{false};
};

// Get the global StreamManager instance.
StreamManager* getStreamManager(mongo::ServiceContext* svcCtx);

}  // namespace streams
