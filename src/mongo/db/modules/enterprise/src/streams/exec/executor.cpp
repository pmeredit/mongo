/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/executor.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
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

    _options.operatorDag->stop();
}

StreamSummaryStats Executor::getSummaryStats() {
    stdx::lock_guard<Latch> lock(_mutex);

    StreamSummaryStats stats;
    if (_streamStats.operatorStats.empty()) {
        return stats;
    }

    const auto& operatorStats = _streamStats.operatorStats;
    dassert(operatorStats.size() >= 2);
    stats.numInputDocs = operatorStats.begin()->numInputDocs;
    stats.numInputBytes = operatorStats.begin()->numInputBytes;
    // Output docs/bytes are input docs/bytes for the sink.
    stats.numOutputDocs = operatorStats.rbegin()->numInputDocs;
    stats.numOutputBytes = operatorStats.rbegin()->numInputBytes;
    return stats;
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

int32_t Executor::runOnce() {
    auto source = dynamic_cast<SourceOperator*>(_options.operatorDag->source());
    dassert(source);
    return source->runOnce();
}

void Executor::runLoop() {
    auto source = dynamic_cast<SourceOperator*>(_options.operatorDag->source());
    dassert(source);
    auto sink = dynamic_cast<SinkOperator*>(_options.operatorDag->sink());
    dassert(sink);
    while (true) {
        {
            stdx::lock_guard<Latch> lock(_mutex);
            if (_checkpointControlMsg) {
                // During the stop flow, the StreamManager expects the checkpoint message
                // to be processed before the Executor shuts down.
                LOGV2_INFO(75950,
                           "Starting checkpoint",
                           "checkpointId"_attr = _checkpointControlMsg->checkpointMsg->id);
                source->onControlMsg(0 /* inputIdx */, std::move(*_checkpointControlMsg));
                _checkpointControlMsg = boost::none;
            }

            if (_shutdown) {
                LOGV2_INFO(75896,
                           "{streamProcessorName}: exiting runLoop()",
                           "streamProcessorName"_attr = _options.streamProcessorName);
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
        }

        bool docsFlushed = runOnce();
        if (docsFlushed) {
            if (_options.sourceNotIdleSleepDurationMs) {
                stdx::this_thread::sleep_for(
                    stdx::chrono::milliseconds(_options.sourceNotIdleSleepDurationMs));
            }
        } else {
            // No docs were flushed in this run, so sleep a little before starting
            // the next run.
            // TODO: add jitter
            stdx::this_thread::sleep_for(
                stdx::chrono::milliseconds(_options.sourceIdleSleepDurationMs));
        }
    }
}

void Executor::insertControlMsg(StreamControlMsg controlMsg) {
    stdx::lock_guard<Latch> lock(_mutex);
    uassert(75810,
            "Expected checkpoint controlMsg",
            controlMsg.checkpointMsg && !controlMsg.watermarkMsg);
    _checkpointControlMsg = std::move(controlMsg);
}

}  // namespace streams
