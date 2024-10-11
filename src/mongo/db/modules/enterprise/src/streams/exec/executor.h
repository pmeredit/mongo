/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <chrono>
#include <memory>
#include <queue>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/future.h"
#include "mongo/util/producer_consumer_queue.h"
#include "mongo/util/timer.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/exec/checkpoint_coordinator.h"
#include "streams/exec/common_gen.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/stream_stats.h"
#include "streams/exec/tenant_feature_flags.h"
#include "streams/util/metric_manager.h"
#include "streams/util/metrics.h"

namespace streams {

class DeadLetterQueue;
class OperatorDag;
class OutputSampler;
struct Context;

/**
 * This class executes an OperatorDag. The thread in this class is the one on which all
 * the data flow between Operators occur. This class is not thread-safe.
 */
class Executor {
public:
    struct Options {
        OperatorDag* operatorDag{nullptr};
        CheckpointCoordinator* checkpointCoordinator{nullptr};
        // The MetricManager instance to use for this Executor's metrics.
        std::unique_ptr<MetricManager> metricManager;
        // Sleep duration when source is idle.
        int32_t sourceIdleSleepDurationMs{100};
        // Sleep duration when source is not idle.
        // This is currently always zero except when a sample data source is used.
        int32_t sourceNotIdleSleepDurationMs{0};
        // Whether the executor should send one last CheckpointControlMsg through the OperatorDag
        // before shutting down.
        bool sendCheckpointControlMsgBeforeShutdown{true};
        // Maximum time allowed to set up the initial connections.
        mongo::Seconds connectTimeout{60};
        // Maximum time allowed to gracefully stop the stream processor.
        mongo::Seconds stopTimeout{10 * 60};
        // Whether to let data flow through the Operator DAG after it is started.
        bool enableDataFlow{true};
        // Max amount of bytes that can be buffered in the in-memory buffer. Once
        // this threshold is hit, all subsequent inserts will block until there is
        // space available.
        int64_t testOnlyDocsQueueMaxSizeBytes{512 * 1024 * 1024};  // 512 MB
    };

    Executor(Context* context, Options options);

    ~Executor();

    // Starts the OperatorDag and _executorThread.
    // Returns a Future that would be completed with an error when the stream processor runs into
    // an error.
    mongo::Future<void> start();

    // Stops the OperatorDag and _executorThread.
    void stop(StopReason stopReason);

    // True if the Operators have succesfully connected.
    bool isConnected();

    // Returns stats for each operator.
    std::vector<OperatorStats> getOperatorStats();

    // Returns the state for each kafka consumer partition. If this stream processor is not using
    // the kafka consumer source, then this returns an empty vector.
    std::vector<KafkaConsumerPartitionState> getKafkaConsumerPartitionStates() const;

    // Adds an OutputSampler to register with the SinkOperator.
    void addOutputSampler(boost::intrusive_ptr<OutputSampler> sampler);

    // Test-only method to insert documents into a stream that uses InMemorySourceOperator as the
    // source.
    void testOnlyInsertDocuments(std::vector<mongo::BSONObj> docs);

    // Test-only method to inject an exception into runLoop().
    void testOnlyInjectException(std::exception_ptr exception);

    // this is made available for tests
    MetricManager* getMetricManager() {
        return _options.metricManager.get();
    }

    // Returns a description of the restore checkpoint, if there is one.
    boost::optional<mongo::CheckpointDescription> getRestoredCheckpointDescription();

    // Returns a description of the last committed checkpoint, if there is one.
    boost::optional<mongo::CheckpointDescription> getLastCommittedCheckpointDescription();

    // The first component of the returned pair is the current resume token or
    // startAtOperationTimestamp for a change stream $source. The second component is the delta
    // between the timestamp of the last event in the oplog and the timestamp in the last obtained
    // resume token from the change stream.
    std::pair<boost::optional<std::variant<mongo::BSONObj, mongo::Timestamp>>, mongo::Seconds>
    getChangeStreamState() const;

    // Get feature flags set for this context.
    mongo::BSONObj testOnlyGetFeatureFlags() const;

    // To notify the executor that the feature flags have been updated.
    void onFeatureFlagsUpdated(std::shared_ptr<TenantFeatureFlags> tenantFeatureFlags);

    // This gets invoked via a RPC command to get this SP to take a checkpoint. If force is true,
    // then it leads to bypassing the normal logic in the checkpoint coordinator around skipping
    // checkpoints if nothing has changed. If force is false, it will cause a checkpoint to be
    // written as long as some state has changed since the last checkpoint
    void writeCheckpoint(bool force);

    // Called by the StreamManager when the streams Agent has flushed a checkpoint to remote
    // storage.
    void onCheckpointFlushed(CheckpointId checkpointId);

private:
    friend class CheckpointTestWorkload;
    friend class CheckpointTest;
    friend class StreamManagerTest;

    enum class RunStatus {
        kActive,
        kIdle,
        kShutdown,
        kShuttingDown,
    };

    // Cost function for the bounded test only docs MPSC queue.
    struct TestOnlyDocsQueueCostFunc {
        size_t operator()(const std::vector<mongo::BSONObj>& objs) const {
            size_t out{0};
            for (const auto& obj : objs) {
                out += obj.objsize();
            }
            return out;
        }
    };

    // Called repeatedly by runLoop() to do the actual work.
    // Returns the number of documents read from the source in this run.
    RunStatus runOnce();

    // _executorThread uses this to continuously read documents from the source operator of the
    // OperatorDag and get them sent through the OperatorDag.
    void runLoop();

    // Sends the given CheckpointControlMsg through the OperatorDag.
    void sendCheckpointControlMsg(CheckpointControlMsg msg);

    // Ensures that connections with the source and the sink are successfully established
    // within the given deadline.
    // This method is called once at the beginning of the Executor's background thread.
    void ensureConnected(mongo::Date_t deadline);

    // Takes the mutex and checks for _shutdown.
    bool isShutdown();

    // This is called when a checkpoint has been flushed to remote storage.
    void processFlushedCheckpoint(mongo::CheckpointDescription checkpointDescription);

    // Process any checkpoints that have been flushed to remote storage.
    std::deque<CheckpointId> processFlushedCheckpoints();

    // Update stream processor feature flags for this context.
    void updateContextFeatureFlags();

    // Updates all the stats, including the $source state. Called once per runOnce.
    void updateStats();

    // Context of the streamProcessor, used for logging purposes.
    Context* _context{nullptr};
    Options _options;
    mongo::Timer _executorTimer;
    mongo::Promise<void> _promise;
    mongo::stdx::thread _executorThread;
    mutable mongo::stdx::mutex _mutex;
    bool _shutdown{false};
    StopReason _stopReason;
    mongo::Date_t _stopDeadline;
    // During stop we write a final checkpoint and set this member variable.
    boost::optional<CheckpointId> _lastCheckpointId;
    bool _connected{false};


    // The last snapshot of the stats after a batch is finished.
    // NOTE: This and other stats like _kafkaConsumerPartitionStates might be retrieved
    // after the executor is stopped.
    StreamStats _streamStats;

    // Only applicable if the stream processor has a kafka source.
    //
    // Snapshot of the last states of each kafka source partition, which is snapshotted every
    // `runOnce()` iteration. This is snapshotted at the same time as `_streamStats` so this
    // will be consistent with the stats that we hold.
    std::vector<KafkaConsumerPartitionState> _kafkaConsumerPartitionStates;

    // A description of the checkpoint that this Executor restores from.
    boost::optional<mongo::CheckpointDescription> _restoredCheckpointDescription;
    // A description of the last committed checkpoint.
    boost::optional<mongo::CheckpointDescription> _lastCommittedCheckpointDescription;

    std::vector<boost::intrusive_ptr<OutputSampler>> _outputSamplers;
    boost::optional<std::exception_ptr> _testOnlyException;

    // Documents are inserted into this queue from `testOnlyInsert` potentially be multiple
    // producers, and are only consumed by the single threaded executor run loop.
    mongo::MultiProducerSingleConsumerQueue<std::vector<mongo::BSONObj>, TestOnlyDocsQueueCostFunc>
        _testOnlyDocsQueue;

    // Set of metrics that are periodically updated in the run loop when operator
    // stats are polled.
    std::shared_ptr<Counter> _numInputDocumentsCounter;
    std::shared_ptr<Counter> _numInputBytesCounter;
    std::shared_ptr<Counter> _numOutputDocumentsCounter;
    std::shared_ptr<Counter> _numOutputBytesCounter;
    std::shared_ptr<Counter> _runOnceCounter;
    std::shared_ptr<IntGauge> _memoryUsageGauge;
    std::shared_ptr<IntGauge> _startDurationGauge;
    std::shared_ptr<IntGauge> _stopDurationGauge;
    std::shared_ptr<IntGauge> _maxRunOnceDurationGauge;

    // Have some new outputdocs been emitted by _any_ operator since we last checked.
    // This is used in determining if taking a newer checkpoint can be safely skipped
    bool _uncheckpointedState{false};

    // Is there a pending external writeCheckpoint request
    mongo::AtomicWord<WriteCheckpointCommand> _writeCheckpointCommand{
        WriteCheckpointCommand::kNone};
    // The current resume token or timestamp for a change stream $source.
    boost::optional<std::variant<mongo::BSONObj, mongo::Timestamp>> _changeStreamState;
    mongo::Seconds _changeStreamLag;
    std::shared_ptr<TenantFeatureFlags> _tenantFeatureFlagsUpdate;

    // A queue of checkpointIDs that have been flushed to remote storage.
    std::deque<CheckpointId> _checkpointFlushEvents;
};

}  // namespace streams
