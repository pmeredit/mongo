/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include <functional>
#include <memory>

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/duration.h"
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
            ErrorCodes::InvalidOptions,
            str::stream() << "Only in-memory or fake kafka sources supported for testOnlyInsert");
    }
}

}  // namespace

Executor::Executor(Context* context, Options options)
    : _context(context),
      _options(std::move(options)),
      _testOnlyDocsQueue(decltype(_testOnlyDocsQueue)::Options{
          .maxQueueDepth = static_cast<size_t>(_options.testOnlyDocsQueueMaxSizeBytes)}) {
    _metricManager = std::make_unique<MetricManager>();
    auto labels = getDefaultMetricLabels(_context);

    _numInputDocumentsCounter = _metricManager->registerCounter(
        "num_input_documents", "Number of input documents received from a source", labels);
    _numInputBytesCounter = _metricManager->registerCounter(
        "num_input_bytes", "Number of input bytes received from a source", labels);
    _numOutputDocumentsCounter = _metricManager->registerCounter(
        "num_output_documents", "Number of documents emitted from the stream processor", labels);
    _numOutputBytesCounter = _metricManager->registerCounter(
        "num_output_bytes", "Number of bytes emitted from the stream processor", labels);
    _tenantFeatureFlags = std::move(_options.tenantFeatureFlags);
}

Executor::~Executor() {
    // make sure that stop() has already been called if necessary.
    dassert(!_executorThread.joinable());
}

Future<void> Executor::start() {
    auto pf = makePromiseFuture<void>();
    _promise = std::move(pf.promise);

    // Start the executor thread.
    dassert(!_executorThread.joinable());
    _executorThread = stdx::thread([this] {
        bool promiseFulfilled{false};
        try {
            if (_context->checkpointStorage) {
                _context->checkpointStorage->registerMetrics(_metricManager.get());
                // Register a post commit callback to commit kafka offsets after a checkpoint
                // has been committed.
                // Currently checkpoint commit happens in the main Executor thread, and so does the
                // callback.
                _context->checkpointStorage->registerPostCommitCallback(
                    [this](mongo::CheckpointDescription checkpointDescription) {
                        onCheckpointCommitted(std::move(checkpointDescription));
                    });
            }

            _context->dlq->registerMetrics(_metricManager.get());
            // Start the DLQ.
            _context->dlq->start();

            const auto& operators = _options.operatorDag->operators();
            for (const auto& oper : operators) {
                oper->registerMetrics(_metricManager.get());
            }

            // Start the OperatorDag.
            LOGV2_INFO(76451, "starting operator dag", "context"_attr = _context);
            _options.operatorDag->start();

            Date_t deadline = Date_t::now() + _options.connectTimeout;
            ensureConnected(deadline);

            if (_context->checkpointStorage && _context->restoreCheckpointId) {
                tassert(8444407,
                        "Expected a restoreCheckpointDescription",
                        _context->restoredCheckpointDescription);
                _context->restoredCheckpointDescription->setSourceState(
                    _options.operatorDag->source()->getRestoredState());
                auto duration =
                    _context->checkpointStorage->checkpointRestored(*_context->restoreCheckpointId);
                _context->restoredCheckpointDescription->setRestoreDurationMs(duration);

                {
                    stdx::lock_guard<Latch> lock(_mutex);
                    _restoredCheckpointDescription = _context->restoredCheckpointDescription;
                }
            }

            LOGV2_INFO(76452, "started operator dag", "context"_attr = _context);
            {
                stdx::lock_guard<Latch> lock(_mutex);
                _started = true;
            }

            runLoop();
        } catch (const SPException& e) {
            LOGV2_WARNING(75900,
                          "encountered stream processor exception, exiting runLoop(): {error}",
                          "context"_attr = _context,
                          "errorCode"_attr = e.code(),
                          "reason"_attr = e.reason(),
                          "unsafeErrorMessage"_attr = e.unsafeReason(),
                          "error"_attr = e.what());
            _promise.setError(e.toStatus());
            promiseFulfilled = true;
        } catch (const DBException& e) {
            LOGV2_WARNING(75899,
                          "encountered exception, exiting runLoop(): {error}",
                          "context"_attr = _context,
                          "errorCode"_attr = e.code(),
                          "reason"_attr = e.reason(),
                          "error"_attr = e.what());
            // Propagate this error to StreamManager. This error will be returned to the user.
            // We assume all DBExceptions have safe error messages to return to the user.
            _promise.setError(e.toStatus());
            promiseFulfilled = true;
        } catch (const std::exception& e) {
            LOGV2_ERROR(75897,
                        "encountered exception, exiting runLoop(): {error}",
                        "context"_attr = _context,
                        "error"_attr = e.what());
            // This error will go back to the user. std::exception error messages
            // are not safe to return to users.
            _promise.setError(Status(ErrorCodes::Error(75385), "An internal error occured."));
            promiseFulfilled = true;
        }

        if (!promiseFulfilled) {
            // Fulfill the promise as the thread exits.
            _promise.emplaceValue();
        }
    });

    return std::move(pf.future);
}

void Executor::stop(StopReason stopReason) {
    // Stop the executor thread.
    bool joinThread{false};
    if (_executorThread.joinable()) {
        stdx::lock_guard<Latch> lock(_mutex);
        _shutdown = true;
        _stopReason = stopReason;
        joinThread = true;
    }
    if (joinThread) {
        // Wait for the executor thread to exit.
        _executorThread.join();
    }

    LOGV2_INFO(76431, "stopping operator dag", "context"_attr = _context);
    _options.operatorDag->stop();
    _context->dlq->stop();
    _testOnlyDocsQueue.closeConsumerEnd();
}

std::vector<OperatorStats> Executor::getOperatorStats() {
    stdx::lock_guard<Latch> lock(_mutex);
    return _streamStats.operatorStats;
}

std::vector<KafkaConsumerPartitionState> Executor::getKafkaConsumerPartitionStates() const {
    stdx::lock_guard<Latch> lock(_mutex);
    return _kafkaConsumerPartitionStates;
}

void Executor::addOutputSampler(boost::intrusive_ptr<OutputSampler> sampler) {
    stdx::lock_guard<Latch> lock(_mutex);
    dassert(sampler);
    _outputSamplers.push_back(std::move(sampler));
}

boost::optional<mongo::CheckpointDescription> Executor::getLastCommittedCheckpointDescription() {
    stdx::lock_guard<Latch> lock(_mutex);
    return _lastCommittedCheckpointDescription;
}

boost::optional<mongo::CheckpointDescription> Executor::getRestoredCheckpointDescription() {
    stdx::lock_guard<Latch> lock(_mutex);
    return _restoredCheckpointDescription;
}

void Executor::testOnlyInsertDocuments(std::vector<mongo::BSONObj> docs) {
    // Ignore empty messages.
    if (!docs.empty()) {
        _testOnlyDocsQueue.push(std::move(docs));
    }
}

void Executor::testOnlyInjectException(std::exception_ptr exception) {
    stdx::lock_guard<Latch> lock(_mutex);
    _testOnlyException = std::move(exception);
}

bool Executor::isStarted() {
    stdx::lock_guard<Latch> lock(_mutex);
    return _started;
}

boost::optional<std::variant<mongo::BSONObj, mongo::Timestamp>> Executor::getChangeStreamState()
    const {
    stdx::lock_guard<Latch> lock(_mutex);
    return _changeStreamState;
}

void Executor::writeCheckpoint(bool force) {
    if (force) {
        _writeCheckpointCommand.store(WriteCheckpointCommand::kForce);
    } else {
        _writeCheckpointCommand.store(WriteCheckpointCommand::kNormal);
    }
}

Executor::RunStatus Executor::runOnce() {
    auto source = dynamic_cast<SourceOperator*>(_options.operatorDag->source());
    dassert(source);
    auto sink = dynamic_cast<SinkOperator*>(_options.operatorDag->sink());
    dassert(sink);
    auto checkpointCoordinator = _options.checkpointCoordinator;
    bool shutdown{false};
    StopReason stopReason;

    // Ensure the source is still connected. If not, throw an error.
    auto connectionStatus = source->getConnectionStatus();
    spassert(std::move(connectionStatus.error), connectionStatus.isConnected());

    auto sinkStatus = sink->getConnectionStatus();
    spassert(std::move(sinkStatus.error), sinkStatus.isConnected());

    auto dlqStatus = _context->dlq->getStatus();
    spassert(std::move(dlqStatus), dlqStatus.isOK());

    // Check if this stream processor needs to potentially be killed if this process
    // is running out of memory.
    _context->memoryAggregator->poll();

    do {
        stdx::lock_guard<Latch> lock(_mutex);

        // Only shutdown if the inserted test documents have all been processed.
        if (_shutdown && _testOnlyDocsQueue.getStats().queueDepth == 0) {
            shutdown = true;
            stopReason = _stopReason;
            break;
        }

        // Update _streamStats with the latest stats.
        StreamStats streamStats;
        const auto& operators = _options.operatorDag->operators();
        for (const auto& oper : operators) {
            streamStats.operatorStats.push_back(oper->getStats());
        }
        updateStats(std::move(streamStats));
        _metricManager->takeSnapshot();
        updateContextFeatureFlags();

        if (const auto* source =
                dynamic_cast<KafkaConsumerOperator*>(_options.operatorDag->source())) {
            _kafkaConsumerPartitionStates = source->getPartitionStates();
        }

        if (const auto* source =
                dynamic_cast<ChangeStreamSourceOperator*>(_options.operatorDag->source())) {
            _changeStreamState = source->getCurrentState();
        }

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
        LOGV2_INFO(8728300,
                   "executor shutting down",
                   "context"_attr = _context,
                   "stopReason"_attr = stopReasonToString(stopReason));

        if (checkpointCoordinator && _options.sendCheckpointControlMsgBeforeShutdown) {
            auto checkpointControlMsg = checkpointCoordinator->getCheckpointControlMsgIfReady(
                CheckpointCoordinator::CheckpointRequest{
                    .changeStreamAdvanced = changeStreamAdvanced,
                    .uncheckpointedState = _uncheckpointedState,
                    .writeCheckpointCommand = _writeCheckpointCommand.load(),
                    .shutdown = true});
            if (checkpointControlMsg) {
                sendCheckpointControlMsg(std::move(*checkpointControlMsg));
            }
        }
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
            LOGV2_DEBUG(8017802,
                        2,
                        "sending checkpointcontrolmsg",
                        "uncheckpointedState"_attr = _uncheckpointedState,
                        "changeStreamAdvanced"_attr = changeStreamAdvanced,
                        "req"_attr = _writeCheckpointCommand.load(),
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

void Executor::updateStats(StreamStats newStats) {
    auto prevSummary = computeStreamSummaryStats(_streamStats.operatorStats);
    auto newSummary = computeStreamSummaryStats(newStats.operatorStats);
    auto delta = newSummary - prevSummary;

    _numInputDocumentsCounter->increment(delta.numInputDocs);
    _numInputBytesCounter->increment(delta.numInputBytes);
    _numOutputDocumentsCounter->increment(delta.numOutputDocs);
    _numOutputBytesCounter->increment(delta.numOutputBytes);

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
}

void Executor::onCheckpointCommitted(mongo::CheckpointDescription checkpointDescription) {
    // Note that currently this gets called when the checkpoint is written to the local disk. As
    // part of SERVER-82562, we will have a way to know when the checkpoint is uploaded to the cloud
    // storage and not just written to local disk. In that case, there might still be actions we
    // need to take when the write to local disk finishes (i.e. the take-a-checkpoint control
    // message propagates through the DAG to the end).

    auto source = _options.operatorDag->source();
    source->onCheckpointCommit(checkpointDescription.getId());
    _uncheckpointedState = false;

    if (_context->checkpointStorage) {
        checkpointDescription.setSourceState(
            _options.operatorDag->source()->getLastCommittedState());
        {
            stdx::lock_guard<Latch> lock(_mutex);
            _lastCommittedCheckpointDescription = std::move(checkpointDescription);
        }
    }
}

bool Executor::isShutdown() {
    stdx::lock_guard<Latch> lock(_mutex);
    return _shutdown;
}

void Executor::ensureConnected(Date_t deadline) {
    // TODO(SERVER-80742): Establish connection with sinks and DLQs.
    // Connect to the source.
    auto source = dynamic_cast<SourceOperator*>(_options.operatorDag->source());
    auto sink = dynamic_cast<SinkOperator*>(_options.operatorDag->sink());
    invariant(source);
    // Even .process pipelines, which don't require a sink, get a NoOp sink appended.
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
            break;
        } else if (sourceStatus.isError()) {
            throw SPException{std::move(sourceStatus.error)};
        } else if (sinkStatus.isError()) {
            throw SPException{std::move(sinkStatus.error)};
        }

        // The source or sink has not finished connecting yet, and has neither have errored.
        uassert(75380, "Timeout while connecting.", Date_t::now() <= deadline);
        // Sleep for a bit before calling connect again.
        sleepFor(sleepDuration);
    }
}

void Executor::runLoop() {
    invariant(_started);

    while (true) {
        RunStatus status = runOnce();
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
            case RunStatus::kShutdown:
                LOGV2_INFO(75896, "exiting runLoop() after shutdown", "context"_attr = _context);
                return;
        }
    }
}

BSONObj Executor::testOnlyGetFeatureFlags() const {
    stdx::lock_guard<Latch> lock(_mutex);
    mongo::MutableDocument doc;
    if (_context->featureFlags) {
        for (auto [k, v] : _context->featureFlags->testOnlyGetFeatureFlags()) {
            doc.addField(k, v);
        }
    }
    return doc.freeze().toBson();
}


void Executor::updateContextFeatureFlags() {
    if (_featureFlagsUpdated) {
        if (!_context->featureFlags) {
            _context->featureFlags =
                _tenantFeatureFlags->getStreamProcessorFeatureFlags(_context->streamName);
        } else {
            _context->featureFlags->updateFeatureFlags(
                _tenantFeatureFlags->getStreamProcessorFeatureFlags(_context->streamName));
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
        _featureFlagsUpdated = false;
    }
}

void Executor::onFeatureFlagsUpdated() {
    stdx::lock_guard<Latch> lock(_mutex);
    _featureFlagsUpdated = true;
}

}  // namespace streams
