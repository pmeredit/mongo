/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include <functional>
#include <memory>

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/time_support.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/exec/change_stream_source_operator.h"
#include "streams/exec/checkpoint_coordinator.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/executor.h"
#include "streams/exec/feature_flag.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/log_util.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/sink_operator.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/tenant_feature_flags.h"
#include "streams/util/exception.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

namespace {

// If enabled, executor will sleep for specified duration before attempting source/sink connections.
MONGO_FAIL_POINT_DEFINE(streamProcessorStartSleepSeconds);
// If enabled, executor will sleep for specified duration while stopping the stream processor.
MONGO_FAIL_POINT_DEFINE(streamProcessorStopSleepSeconds);

void testOnlyInsert(SourceOperator* source, std::vector<mongo::BSONObj> inputDocs) {
    dassert(source);
    if (auto inMemorySource = dynamic_cast<InMemorySourceOperator*>(source)) {
        StreamDataMsg dataMsg;
        dataMsg.docs.reserve(inputDocs.size());
        for (auto& doc : inputDocs) {
            dassert(doc.isOwned());
            dataMsg.docs.emplace_back(Document(doc));
        }
        inMemorySource->addDataMsg(std::move(dataMsg));
    } else if (auto fakeKafkaSource = dynamic_cast<KafkaConsumerOperator*>(source)) {
        fakeKafkaSource->testOnlyInsertDocuments(std::move(inputDocs));
    } else {
        uasserted(
            ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "Only in-memory or fake kafka sources supported for testOnlyInsert");
    }
}

}  // namespace

Executor::Executor(Context* context, Options options)
    : _context(context),
      _options(std::move(options)),
      _testOnlyDocsQueue(decltype(_testOnlyDocsQueue)::Options{
          .maxQueueDepth = static_cast<size_t>(_options.testOnlyDocsQueueMaxSizeBytes)}) {
    auto labels = getDefaultMetricLabels(_context);

    _numInputDocumentsCounter = _options.metricManager->registerCounter(
        "num_input_documents", "Number of input documents received from a source", labels);
    _numInputBytesCounter = _options.metricManager->registerCounter(
        "num_input_bytes", "Number of input bytes received from a source", labels);
    _numOutputDocumentsCounter = _options.metricManager->registerCounter(
        "num_output_documents", "Number of documents emitted from the stream processor", labels);
    _numOutputBytesCounter = _options.metricManager->registerCounter(
        "num_output_bytes", "Number of bytes emitted from the stream processor", labels);
    _runOnceCounter = _options.metricManager->registerCounter(
        "runonce_count", "Number of runOnce iterations in the stream processor", labels);
    _memoryUsageGauge = _options.metricManager->registerIntGauge(
        "memory_usage_bytes",
        "Current memory usage based on the internal memory usage tracking",
        labels);
    _startDurationGauge = _options.metricManager->registerIntGauge(
        "start_duration_ms", "Time taken to start the stream processor in milliseconds", labels);
    _stopDurationGauge = _options.metricManager->registerIntGauge(
        "stop_duration_ms", "Time taken to stop the stream processor in milliseconds", labels);
    _maxRunOnceDurationGauge = _options.metricManager->registerIntGauge(
        "max_runonce_duration_ms", "Maximum time taken by runOnce() in milliseconds", labels);
}

Executor::~Executor() {
    if (_executorThread.joinable()) {
        // Wait for the executor thread to exit.
        _executorThread.join();
    }
}

Future<void> Executor::start() {
    auto pf = makePromiseFuture<void>();
    _promise = std::move(pf.promise);

    // Start the executor thread.
    dassert(!_executorThread.joinable());
    _executorThread = stdx::thread([this] {
        _executorTimer.reset();
        bool started{false};
        bool promiseFulfilled{false};
        Date_t deadline = Date_t::now() + _options.connectTimeout;
        try {
            _context->dlq->registerMetrics(_options.metricManager.get());
            // Start the DLQ.
            _context->dlq->start();

            const auto& operators = _options.operatorDag->operators();
            for (const auto& oper : operators) {
                oper->registerMetrics(_options.metricManager.get());
            }

            // Start the OperatorDag.
            LOGV2_INFO(76451, "starting operator dag", "context"_attr = _context);
            _options.operatorDag->start();
            started = true;
            _startDurationGauge->set(_executorTimer.millis());
            _executorTimer.reset();
            LOGV2_INFO(76452, "started operator dag", "context"_attr = _context);

            if (auto fp = streamProcessorStartSleepSeconds.scoped(); fp.isActive()) {
                auto sleepSeconds = static_cast<int64_t>(fp.getData()["sleepSeconds"].numberLong());
                sleepFor(Seconds{sleepSeconds});
            }

            ensureConnected(deadline);

            if (isConnected()) {
                if (_context->checkpointStorage && _context->restoreCheckpointId) {
                    tassert(8444407,
                            "Expected a restoreCheckpointDescription",
                            _context->restoredCheckpointInfo);
                    auto description = _context->restoredCheckpointInfo->description;
                    description.setSourceState(_options.operatorDag->source()->getRestoredState());
                    auto duration = _context->checkpointStorage->checkpointRestored(
                        *_context->restoreCheckpointId);
                    description.setRestoreDurationMs(duration);
                    {
                        stdx::lock_guard<stdx::mutex> lock(_mutex);
                        _restoredCheckpointDescription = std::move(description);
                    }
                }

                runLoop();
            }
        } catch (...) {
            auto status = exceptionToSPStatus();
            LOGV2_INFO(75900,
                       "Executor encountered exception",
                       "context"_attr = _context,
                       "errorCode"_attr = status.code(),
                       "reason"_attr = status.reason(),
                       "unsafeErrorMessage"_attr = status.unsafeReason());
            _promise.setError(status);
            promiseFulfilled = true;
        }

        if (!promiseFulfilled) {
            // Fulfill the promise as the thread exits.
            _promise.emplaceValue();
        }

        if (started) {
            try {
                LOGV2_INFO(76431, "stopping operator dag", "context"_attr = _context);
                _options.operatorDag->stop();
                LOGV2_INFO(76433, "stopped operator dag", "context"_attr = _context);
            } catch (...) {
                auto status = exceptionToSPStatus();
                LOGV2_WARNING(75901,
                              "encountered exception while stopping OperatorDag",
                              "context"_attr = _context,
                              "errorCode"_attr = status.code(),
                              "reason"_attr = status.reason(),
                              "unsafeErrorMessage"_attr = status.unsafeReason());
            }

            try {
                LOGV2_INFO(8853600, "stopping DLQ", "context"_attr = _context);
                _context->dlq->stop();
                _testOnlyDocsQueue.closeConsumerEnd();
                LOGV2_INFO(8853601, "stopped DLQ", "context"_attr = _context);
            } catch (...) {
                auto status = exceptionToSPStatus();
                LOGV2_WARNING(8853602,
                              "encountered exception while stopping DLQ",
                              "context"_attr = _context,
                              "errorCode"_attr = status.code(),
                              "reason"_attr = status.reason(),
                              "unsafeErrorMessage"_attr = status.unsafeReason());
            }
        }
    });

    return std::move(pf.future);
}

void Executor::stop(StopReason stopReason) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _shutdown = true;
    _stopReason = stopReason;
    _stopDeadline = Date_t::now() + _options.stopTimeout;
}

std::vector<OperatorStats> Executor::getOperatorStats() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _streamStats.operatorStats;
}

std::vector<KafkaConsumerPartitionState> Executor::getKafkaConsumerPartitionStates() const {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _kafkaConsumerPartitionStates;
}

void Executor::addOutputSampler(boost::intrusive_ptr<OutputSampler> sampler) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    dassert(sampler);
    _outputSamplers.push_back(std::move(sampler));
}

boost::optional<mongo::CheckpointDescription> Executor::getLastCommittedCheckpointDescription() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _lastCommittedCheckpointDescription;
}

boost::optional<mongo::CheckpointDescription> Executor::getRestoredCheckpointDescription() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _restoredCheckpointDescription;
}

void Executor::testOnlyInsertDocuments(std::vector<mongo::BSONObj> docs) {
    // Ignore empty messages.
    if (!docs.empty()) {
        _testOnlyDocsQueue.push(std::move(docs));
    }
}

void Executor::testOnlyInjectException(std::exception_ptr exception) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _testOnlyException = std::move(exception);
}

bool Executor::isConnected() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _connected;
}

std::pair<boost::optional<std::variant<mongo::BSONObj, mongo::Timestamp>>, mongo::Seconds>
Executor::getChangeStreamState() const {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return {_changeStreamState, _changeStreamLag};
}

void Executor::writeCheckpoint(bool force) {
    if (force) {
        _writeCheckpointCommand.store(WriteCheckpointCommand::kForce);
    } else {
        _writeCheckpointCommand.store(WriteCheckpointCommand::kNormal);
    }
}

void Executor::updateStats() {
    // Update _streamStats with the latest stats.
    StreamStats newStats;
    const auto& operators = _options.operatorDag->operators();
    for (const auto& oper : operators) {
        newStats.operatorStats.push_back(oper->getStats());
    }

    auto prevSummary = computeStreamSummaryStats(_streamStats.operatorStats);
    auto newSummary = computeStreamSummaryStats(newStats.operatorStats);
    auto delta = newSummary - prevSummary;

    _numInputDocumentsCounter->increment(delta.numInputDocs);
    _numInputBytesCounter->increment(delta.numInputBytes);
    _numOutputDocumentsCounter->increment(delta.numOutputDocs);
    _numOutputBytesCounter->increment(delta.numOutputBytes);
    _memoryUsageGauge->set(_context->memoryAggregator->getCurrentMemoryUsageBytes());

    if (delta.numOutputDocs || delta.numDlqDocs) {
        // no need to check thru all the individual operators
        _uncheckpointedState = true;
    }

    if (!_uncheckpointedState) {
        // Need to check thru each operators outputdocs stats since the summary
        // only considers that for the sink operator
        for (unsigned i = 0; i < _streamStats.operatorStats.size(); i++) {
            if (newStats.operatorStats[i].numOutputDocs >
                _streamStats.operatorStats[i].numOutputDocs) {
                _uncheckpointedState = true;
                break;
            }
        }
    }
    _streamStats = std::move(newStats);

    _options.metricManager->takeSnapshot();

    if (const auto* source = dynamic_cast<KafkaConsumerOperator*>(_options.operatorDag->source())) {
        _kafkaConsumerPartitionStates = source->getPartitionStates();
    }

    if (const auto* source =
            dynamic_cast<ChangeStreamSourceOperator*>(_options.operatorDag->source())) {
        _changeStreamState = source->getCurrentState();
        _changeStreamLag = source->getChangeStreamLag();
    }
}

Executor::RunStatus Executor::runOnce() {
    auto source = dynamic_cast<SourceOperator*>(_options.operatorDag->source());
    dassert(source);
    auto sink = dynamic_cast<SinkOperator*>(_options.operatorDag->sink());
    dassert(sink);
    auto checkpointCoordinator = _options.checkpointCoordinator;
    bool shutdown{false};

    // Ensure the source is still connected. If not, throw an error.
    auto connectionStatus = source->getConnectionStatus();
    connectionStatus.throwIfNotConnected();

    auto sinkStatus = sink->getConnectionStatus();
    sinkStatus.throwIfNotConnected();

    auto dlqStatus = _context->dlq->getStatus();
    spassert(dlqStatus, dlqStatus.isOK());

    // Check if this stream processor needs to potentially be killed if this process
    // is running out of memory.
    _context->memoryAggregator->poll();

    // Process flushed checkpoints.
    std::deque<CheckpointId> processedCheckpoints;
    if (_context->checkpointStorage) {
        processedCheckpoints = processFlushedCheckpoints();
    }

    do {
        stdx::lock_guard<stdx::mutex> lock(_mutex);

        // Only shutdown if the inserted test documents have all been processed.
        if (_shutdown && _testOnlyDocsQueue.getStats().queueDepth == 0) {
            shutdown = true;
            break;
        }

        // TODO: Update stats less often to reduce the amount of cpu spent on this.
        updateStats();

        updateContextFeatureFlags();

        for (auto& sampler : _outputSamplers) {
            sink->addOutputSampler(sampler);
            _context->dlq->addOutputSampler(sampler);
        }
        _outputSamplers.clear();

        if (_testOnlyException) {
            std::rethrow_exception(*_testOnlyException);
        }

        if (_testOnlyDocsQueue.getStats().queueDepth > 0) {
            auto [testOnlyDocs, _] = _testOnlyDocsQueue.popManyUpTo(kDataMsgMaxByteSize);
            for (auto& docs : testOnlyDocs) {
                testOnlyInsert(source, std::move(docs));
            }
        }
    } while (false);

    bool changeStreamAdvanced = false;
    if (source) {
        if (auto* chg = dynamic_cast<ChangeStreamSourceOperator*>(source)) {
            changeStreamAdvanced = chg->hasUncheckpointedState();
        }
    }

    if (shutdown) {
        StopReason stopReason;
        mongo::Date_t stopDeadline;
        {
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            stopReason = _stopReason;
            stopDeadline = _stopDeadline;
        }

        if (auto fp = streamProcessorStopSleepSeconds.scoped(); fp.isActive()) {
            auto sleepSeconds = static_cast<int64_t>(fp.getData()["sleepSeconds"].numberLong());
            sleepFor(Seconds{sleepSeconds});
        }

        uassert(75388, "Timeout while stopping", Date_t::now() <= stopDeadline);

        if (_lastCheckpointId) {
            // We wrote the last checkpoint in the last runOnce call, check if it's been flushed and
            // processed.
            if (std::find(processedCheckpoints.begin(),
                          processedCheckpoints.end(),
                          *_lastCheckpointId) != processedCheckpoints.end()) {
                LOGV2_INFO(8914502,
                           "Checkpoint is flushed",
                           "context"_attr = _context,
                           "checkpointId"_attr = *_lastCheckpointId);
                return RunStatus::kShutdown;
            }

            // We're still waiting for the last checkpoint to be flushed.
            return RunStatus::kShuttingDown;
        }

        LOGV2_INFO(8728300,
                   "executor shutting down",
                   "context"_attr = _context,
                   "stopReason"_attr = stopReasonToString(stopReason));

        _executorTimer.reset();
        if (checkpointCoordinator && _options.sendCheckpointControlMsgBeforeShutdown) {
            auto checkpointControlMsg = checkpointCoordinator->getCheckpointControlMsgIfReady(
                CheckpointCoordinator::CheckpointRequest{
                    .changeStreamAdvanced = changeStreamAdvanced,
                    .uncheckpointedState = _uncheckpointedState,
                    .writeCheckpointCommand = _writeCheckpointCommand.load(),
                    .shutdown = true});
            if (checkpointControlMsg) {
                ScopeGuard guard(
                    [&] { _context->concurrentCheckpointController->onCheckpointComplete(); });
                sendCheckpointControlMsg(*checkpointControlMsg);
                _lastCheckpointId = checkpointControlMsg->id;
                LOGV2_INFO(8914501,
                           "Waiting for checkpoint to be flushed",
                           "context"_attr = _context,
                           "checkpointId"_attr = _lastCheckpointId);
                return RunStatus::kShuttingDown;
            }
        }
        _stopDurationGauge->set(_executorTimer.millis());
        _executorTimer.reset();
        return RunStatus::kShutdown;
    }

    // Send a new checkpoint message only if source or sink have advanced since last checkpoint
    if (checkpointCoordinator) {
        auto checkpointControlMsg = checkpointCoordinator->getCheckpointControlMsgIfReady(
            CheckpointCoordinator::CheckpointRequest{.changeStreamAdvanced = changeStreamAdvanced,
                                                     .uncheckpointedState = _uncheckpointedState,
                                                     .writeCheckpointCommand =
                                                         _writeCheckpointCommand.load(),
                                                     .shutdown = false});

        if (checkpointControlMsg) {
            ScopeGuard guard(
                [&] { _context->concurrentCheckpointController->onCheckpointComplete(); });
            LOGV2_DEBUG(8017802,
                        2,
                        "sending checkpointcontrolmsg",
                        "uncheckpointedState"_attr = _uncheckpointedState,
                        "changeStreamAdvanced"_attr = changeStreamAdvanced,
                        "req"_attr = _writeCheckpointCommand.load(),
                        "context"_attr = _context,
                        "firstcheckpoint"_attr = checkpointCoordinator->writtenFirstCheckpoint());
            sendCheckpointControlMsg(std::move(*checkpointControlMsg));
        }
    }

    _writeCheckpointCommand.store(WriteCheckpointCommand::kNone);

    int64_t docsConsumed{0};
    if (_options.enableDataFlow) {
        docsConsumed = source->runOnce();
    }
    if (docsConsumed > 0) {
        return RunStatus::kActive;
    }
    return RunStatus::kIdle;
}

void Executor::sendCheckpointControlMsg(CheckpointControlMsg msg) {
    uassert(ErrorCodes::InternalError,
            "Attempting to take a checkpoint while dataflow is disabled",
            _options.enableDataFlow);
    LOGV2_INFO(
        76432, "Starting checkpoint", "checkpointId"_attr = msg.id, "context"_attr = _context);
    auto source = dynamic_cast<SourceOperator*>(_options.operatorDag->source());
    dassert(source);
    source->onControlMsg(0 /* inputIdx */, StreamControlMsg{.checkpointMsg = std::move(msg)});
    _uncheckpointedState = false;
}

void Executor::processFlushedCheckpoint(mongo::CheckpointDescription checkpointDescription) {
    tassert(ErrorCodes::InternalError,
            "Expected checkpointStorage to be set.",
            _context->checkpointStorage);

    LOGV2_INFO(8256200,
               "Executor::onCheckpointFlush",
               "context"_attr = _context,
               "checkpointDescription"_attr = checkpointDescription.toBSON());

    auto source = _options.operatorDag->source();
    auto state = source->onCheckpointFlush(checkpointDescription.getId());

    checkpointDescription.setSourceState(std::move(state));
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _lastCommittedCheckpointDescription = std::move(checkpointDescription);
    }
}

bool Executor::isShutdown() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _shutdown;
}

void Executor::onCheckpointFlushed(CheckpointId checkpointId) {
    tassert(ErrorCodes::InternalError,
            "Expected checkpointStorage to be set in Executor::onCheckpointFlushed.",
            _context->checkpointStorage);
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _checkpointFlushEvents.push_back(checkpointId);
}

std::deque<CheckpointId> Executor::processFlushedCheckpoints() {
    std::deque<CheckpointId> checkpointFlushEvents;
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        std::swap(_checkpointFlushEvents, checkpointFlushEvents);
    }

    // Tell checkpointStorage about these flushed checkpoint events.
    for (CheckpointId flushEvent : checkpointFlushEvents) {
        _context->checkpointStorage->onCheckpointFlushed(flushEvent);
    }
    // Get the descriptions of flushed checkpoints back from storage.
    std::deque<CheckpointDescription> flushedCheckpoints =
        _context->checkpointStorage->getFlushedCheckpoints();
    while (!flushedCheckpoints.empty()) {
        auto description = std::move(flushedCheckpoints.front());
        flushedCheckpoints.pop_front();
        processFlushedCheckpoint(std::move(description));
    }

    return checkpointFlushEvents;
}

void Executor::ensureConnected(Date_t deadline) {
    uassert(75382, "Timeout while connecting", Date_t::now() <= deadline);

    // TODO(SERVER-80742): Establish connection with DLQ.
    // Connect to the source.
    auto source = dynamic_cast<SourceOperator*>(_options.operatorDag->source());
    auto sink = dynamic_cast<SinkOperator*>(_options.operatorDag->sink());
    invariant(source);
    invariant(sink);

    constexpr Milliseconds sleepDuration{100};
    while (!isShutdown()) {
        ConnectionStatus sourceStatus = source->getConnectionStatus();
        if (sourceStatus.isConnecting()) {
            sourceStatus = source->getConnectionStatus();
        }

        ConnectionStatus sinkStatus = sink->getConnectionStatus();
        if (sinkStatus.isConnecting()) {
            sinkStatus = sink->getConnectionStatus();
        }

        if (sourceStatus.isConnected() && sinkStatus.isConnected()) {
            LOGV2_INFO(75381, "succesfully connected", "context"_attr = _context);
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            _connected = true;
            break;
        } else if (sourceStatus.isError()) {
            throw SPException{std::move(sourceStatus.error)};
        } else if (sinkStatus.isError()) {
            throw SPException{std::move(sinkStatus.error)};
        }

        // The source or sink has not finished connecting yet, and has neither have errored.
        uassert(75380, "Timeout while connecting", Date_t::now() <= deadline);
        // Sleep for a bit before calling connect again.
        sleepFor(sleepDuration);
    }
}

void Executor::runLoop() {
    invariant(_connected);

    while (true) {
        Timer timer;
        ScopeGuard guard([&] {
            if (_maxRunOnceDurationGauge->value() < timer.millis()) {
                _maxRunOnceDurationGauge->set(timer.millis());
            }
        });

        RunStatus status = runOnce();
        _runOnceCounter->increment(1);
        switch (status) {
            case RunStatus::kActive:
                if (_options.sourceNotIdleSleepDurationMs) {
                    stdx::this_thread::sleep_for(
                        stdx::chrono::milliseconds(_options.sourceNotIdleSleepDurationMs));
                }
                break;
            case RunStatus::kIdle:
                // No docs were flushed in this run, so sleep a little before starting
                // the next run.
                // TODO: add jitter
                stdx::this_thread::sleep_for(
                    stdx::chrono::milliseconds(_options.sourceIdleSleepDurationMs));
                break;
            case RunStatus::kShuttingDown:
                // During shutdown we've written the final checkpoint and we're waiting for
                // it to be flushed.
                stdx::this_thread::sleep_for(stdx::chrono::milliseconds(100));
                break;
            case RunStatus::kShutdown: {
                LOGV2_INFO(75896, "exiting runLoop() after shutdown", "context"_attr = _context);
                return;
            }
        }
    }
}

BSONObj Executor::testOnlyGetFeatureFlags() const {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    mongo::MutableDocument doc;
    if (_context->featureFlags) {
        for (auto [k, v] : _context->featureFlags->testOnlyGetFeatureFlags()) {
            doc.addField(k, v);
        }
    }
    return doc.freeze().toBson();
}


void Executor::updateContextFeatureFlags() {
    if (!_tenantFeatureFlagsUpdate) {
        return;
    }

    if (!_context->featureFlags) {
        _context->featureFlags =
            _tenantFeatureFlagsUpdate->getStreamProcessorFeatureFlags(_context->streamName);
    } else {
        _context->featureFlags->updateFeatureFlags(
            _tenantFeatureFlagsUpdate->getStreamProcessorFeatureFlags(_context->streamName));
    }
    // normally we would want to just call getFeatureFlagValue to get the value, but we have
    // different defaults, depending on the presence of window.
    if (_context->featureFlags->isOverridden(FeatureFlags::kCheckpointDurationInMs)) {
        auto val =
            _context->featureFlags->getFeatureFlagValue(FeatureFlags::kCheckpointDurationInMs)
                .getInt();
        if (val) {
            _context->checkpointInterval = std::chrono::milliseconds(val.get());
            auto checkpointCoordinator = _options.checkpointCoordinator;
            if (checkpointCoordinator) {
                checkpointCoordinator->setCheckpointInterval(_context->checkpointInterval);
            }
        }
    }
    _tenantFeatureFlagsUpdate.reset();
}

void Executor::onFeatureFlagsUpdated(std::shared_ptr<TenantFeatureFlags> tenantFeatureFlags) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _tenantFeatureFlagsUpdate = std::move(tenantFeatureFlags);
}

}  // namespace streams
