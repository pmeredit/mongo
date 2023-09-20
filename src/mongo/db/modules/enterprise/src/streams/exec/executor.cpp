/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/executor.h"

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/duration.h"
#include "streams/exec/checkpoint_coordinator.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/constants.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/log_util.h"
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

Executor::Executor(Context* context, Options options)
    : _context(context), _options(std::move(options)) {}

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
            Date_t deadline = Date_t::now() + _options.connectTimeout;
            connect(deadline);

            // Start the OperatorDag.
            LOGV2_INFO(76451, "starting operator dag", "context"_attr = _context);
            _options.operatorDag->start();
            LOGV2_INFO(76452, "started operator dag", "context"_attr = _context);
            {
                stdx::lock_guard<Latch> lock(_mutex);
                _started = true;
            }

            runLoop();
        } catch (const DBException& e) {
            LOGV2_WARNING(75899,
                          "encountered exception, exiting runLoop(): {error}",
                          "context"_attr = _context,
                          "errorCode"_attr = e.code(),
                          "reason"_attr = e.reason(),
                          "error"_attr = e.what());
            // Propagate this error to StreamManager.
            _promise.setError(e.toStatus());
            promiseFulfilled = true;
        } catch (const std::exception& e) {
            LOGV2_ERROR(75897,
                        "encountered exception, exiting runLoop(): {error}",
                        "context"_attr = _context,
                        "error"_attr = e.what());
            // Propagate this error to StreamManager.
            _promise.setError(Status(ErrorCodes::Error(75385), e.what()));
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

    LOGV2_INFO(76431, "stopping operator dag", "context"_attr = _context);
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
    _testOnlyDocs.push(std::move(docs));
}

void Executor::testOnlyInjectException(std::exception_ptr exception) {
    stdx::lock_guard<Latch> lock(_mutex);
    _testOnlyException = std::move(exception);
}

bool Executor::isStarted() {
    stdx::lock_guard<Latch> lock(_mutex);
    return _started;
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
    uassert(connectionStatus.errorCode,
            fmt::format("streamProcessor is not connected: {}", connectionStatus.errorReason),
            connectionStatus.isConnected());

    do {
        stdx::lock_guard<Latch> lock(_mutex);

        // Only shutdown if the inserted test documents have all been processed.
        if (_shutdown && _testOnlyDocs.empty()) {
            shutdown = true;
            break;
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
        while (!_testOnlyDocs.empty()) {
            auto testOnlyDocs = std::move(_testOnlyDocs.front());
            _testOnlyDocs.pop();
            testOnlyInsert(source, std::move(testOnlyDocs));
        }
    } while (false);

    if (shutdown) {
        if (checkpointCoordinator && _options.sendCheckpointControlMsgBeforeShutdown) {
            auto checkpointControlMsg =
                checkpointCoordinator->getCheckpointControlMsgIfReady(/*force*/ true);
            sendCheckpointControlMsg(std::move(*checkpointControlMsg));
        }
        return RunStatus::kShutdown;
    }

    // TODO(SERVER-80178): Send a new checkpoint message only if some output docs have been
    // emitted since the last checkpoint.
    if (checkpointCoordinator) {
        auto checkpointControlMsg = checkpointCoordinator->getCheckpointControlMsgIfReady();
        if (checkpointControlMsg) {
            sendCheckpointControlMsg(std::move(*checkpointControlMsg));
        }
    }

    int64_t docsConsumed = source->runOnce();
    if (docsConsumed > 0) {
        return RunStatus::kActive;
    }
    return RunStatus::kIdle;
}

void Executor::sendCheckpointControlMsg(CheckpointControlMsg msg) {
    LOGV2_INFO(
        76432, "Starting checkpoint", "checkpointId"_attr = msg.id, "context"_attr = _context);
    auto source = dynamic_cast<SourceOperator*>(_options.operatorDag->source());
    dassert(source);
    source->onControlMsg(0 /* inputIdx */, StreamControlMsg{.checkpointMsg = std::move(msg)});
}

bool Executor::isShutdown() {
    stdx::lock_guard<Latch> lock(_mutex);
    return _shutdown;
}

void Executor::connect(Date_t deadline) {
    // TODO(SERVER-80742): Establish connection with sinks and DLQs.
    // Connect to the source.
    auto source = dynamic_cast<SourceOperator*>(_options.operatorDag->source());
    invariant(source);
    constexpr Milliseconds sleepDuration{100};
    while (!isShutdown()) {
        ConnectionStatus connectionStatus = source->getConnectionStatus();
        if (connectionStatus.status == ConnectionStatus::Status::kConnecting) {
            source->connect();
            connectionStatus = source->getConnectionStatus();
        }

        if (connectionStatus.status == ConnectionStatus::Status::kConnected) {
            LOGV2_INFO(75381, "succesfully connected", "context"_attr = _context);
            break;
        } else if (connectionStatus.status == ConnectionStatus::Status::kConnecting) {
            uassert(75380, "Timeout while connecting.", Date_t::now() <= deadline);
            // Sleep for a bit before calling connect again.
            sleepFor(sleepDuration);
        } else {
            invariant(connectionStatus.status == ConnectionStatus::Status::kError);
            uasserted(connectionStatus.errorCode, connectionStatus.errorReason);
        }
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
                LOGV2_INFO(75896, "exiting runLoop()", "context"_attr = _context);
                return;
        }
    }
}

}  // namespace streams
