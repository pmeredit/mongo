/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/executor.h"

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/checkpoint_coordinator.h"
#include "streams/exec/constants.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/sink_operator.h"
#include "streams/exec/source_operator.h"

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

Executor::Executor(Options options) : _options(std::move(options)) {}

Executor::~Executor() {
    // make sure that stop() has already been called if necessary.
    dassert(!_executorThread.joinable());
}

Future<void> Executor::start() {
    auto pf = makePromiseFuture<void>();
    _promise = std::move(pf.promise);

    LOGV2_INFO(76430,
               "{streamProcessorName}: starting operator dag",
               "streamProcessorName"_attr = _options.streamProcessorName);
    _options.operatorDag->start();

    // Start the executor thread.
    dassert(!_executorThread.joinable());
    _executorThread = stdx::thread([this] {
        bool promiseFulfilled{false};
        try {
            runLoop();
        } catch (const std::exception& e) {
            LOGV2_ERROR(75897,
                        "{streamProcessorName}: encountered exception, exiting runLoop(): {error}",
                        "streamProcessorName"_attr = _options.streamProcessorName,
                        "error"_attr = e.what());
            // Propagate this error to StreamManager.
            _promise.setError(Status(ErrorCodes::InternalError, e.what()));
            promiseFulfilled = true;
        }

        if (!promiseFulfilled) {
            // Fulfill the promise as the thread exits.
            _promise.emplaceValue();
        }
    });

    return std::move(pf.future);
}

void Executor::stop() {
    // Stop the executor thread.
    bool joinThread{false};
    if (_executorThread.joinable()) {
        stdx::lock_guard<Latch> lock(_mutex);
        _shutdown = true;
        joinThread = true;
    }
    if (joinThread) {
        // Wait for the executor thread to exit.
        _executorThread.join();
    }

    LOGV2_INFO(76431,
               "{streamProcessorName}: stopping operator dag",
               "streamProcessorName"_attr = _options.streamProcessorName);
    _options.operatorDag->stop();
}

std::vector<OperatorStats> Executor::getOperatorStats() {
    stdx::lock_guard<Latch> lock(_mutex);
    return _streamStats.operatorStats;
}

void Executor::addOutputSampler(boost::intrusive_ptr<OutputSampler> sampler) {
    stdx::lock_guard<Latch> lock(_mutex);
    dassert(sampler);
    _outputSamplers.push_back(std::move(sampler));
}

void Executor::testOnlyInsertDocuments(std::vector<mongo::BSONObj> docs) {
    stdx::lock_guard<Latch> lock(_mutex);
    testOnlyInsert(_options.operatorDag->source(), std::move(docs));
}

void Executor::testOnlyInjectException(std::exception_ptr exception) {
    stdx::lock_guard<Latch> lock(_mutex);
    _testOnlyException = std::move(exception);
}

Executor::RunStatus Executor::runOnce() {
    auto source = dynamic_cast<SourceOperator*>(_options.operatorDag->source());
    dassert(source);
    auto sink = dynamic_cast<SinkOperator*>(_options.operatorDag->sink());
    dassert(sink);
    auto checkpointCoordinator = _options.checkpointCoordinator;
    boost::optional<CheckpointControlMsg> checkpointControlMsg;
    bool shutdown{false};
    bool isConnected{false};

    do {
        stdx::lock_guard<Latch> lock(_mutex);
        if (_shutdown) {
            shutdown = true;
            if (_isConnected && checkpointCoordinator &&
                _options.sendCheckpointControlMsgBeforeShutdown) {
                checkpointControlMsg = checkpointCoordinator->getCheckpointControlMsgIfReady(
                    /*force*/ true);
            }
            break;
        }

        // Wait until the source is connected.
        if (!_isConnected) {
            source->connect();
            if (source->isConnected()) {
                LOGV2_INFO(76433,
                           "{streamProcessorName}: source is connected now",
                           "streamProcessorName"_attr = _options.streamProcessorName);
                _isConnected = true;
            } else {
                break;
            }
        }
        isConnected = _isConnected;

        // TODO(SERVER-80178): Send a new checkpoint message only if some output docs have been
        // emitted since the last checkpoint.
        if (_isConnected && checkpointCoordinator) {
            auto checkpointControlMsg = checkpointCoordinator->getCheckpointControlMsgIfReady();
            if (checkpointControlMsg) {
                sendCheckpointControlMsg(std::move(*checkpointControlMsg));
            }
        }

        // Update _streamStats with the latest stats.
        _streamStats = StreamStats{};
        const auto& operators = _options.operatorDag->operators();
        for (const auto& oper : operators) {
            _streamStats.operatorStats.push_back(oper->getStats());
        }

        for (auto& sampler : _outputSamplers) {
            sink->addOutputSampler(std::move(sampler));
        }
        _outputSamplers.clear();

        if (_testOnlyException) {
            std::rethrow_exception(*_testOnlyException);
        }
    } while (false);

    if (checkpointControlMsg) {
        sendCheckpointControlMsg(std::move(*checkpointControlMsg));
    }

    if (shutdown) {
        return RunStatus::kShutdown;
    }

    if (isConnected) {
        int64_t docsConsumed = source->runOnce();
        if (docsConsumed > 0) {
            return RunStatus::kActive;
        }
    }
    return RunStatus::kIdle;
}

void Executor::sendCheckpointControlMsg(CheckpointControlMsg msg) {
    LOGV2_INFO(76432, "Starting checkpoint", "checkpointId"_attr = msg.id);
    auto source = dynamic_cast<SourceOperator*>(_options.operatorDag->source());
    dassert(source);
    source->onControlMsg(0 /* inputIdx */, StreamControlMsg{.checkpointMsg = std::move(msg)});
}

void Executor::runLoop() {
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
                LOGV2_INFO(75896,
                           "{streamProcessorName}: exiting runLoop()",
                           "streamProcessorName"_attr = _options.streamProcessorName);
                return;
        }
    }
}

}  // namespace streams
