/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#include "streams/exec/executor.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/sink_operator.h"
#include "streams/exec/source_operator.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

Executor::Executor(Options options) : _options(std::move(options)) {}

Executor::~Executor() {
    // make sure that stop() has already been called if necessary.
    dassert(!_executorThread.joinable());
}

void Executor::start() {
    _options.operatorDag->start();

    // Start the executor thread.
    dassert(!_executorThread.joinable());
    _executorThread = stdx::thread([this] { runLoop(); });
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

void Executor::addOutputSampler(OutputSampler* sampler) {
    stdx::lock_guard<Latch> lock(_mutex);
    dassert(sampler);
    _outputSamplers.push_back(sampler);
}

int32_t Executor::runOnce() {
    auto source = dynamic_cast<SourceOperator*>(_options.operatorDag->source());
    dassert(source);

    int32_t docsFlushed{0};
    try {
        docsFlushed = source->runOnce();
    } catch (const std::exception& e) {
        // TODO: Propagate this error to the higher layer and also deschedule the stream.
        LOGV2_ERROR(75897,
                    "{streamProcessorName}: encountered exception, exiting runLoop(): {error}",
                    "streamProcessorName"_attr = _options.streamProcessorName,
                    "error"_attr = e.what());
    }
    return docsFlushed;
}

void Executor::runLoop() {
    auto sink = dynamic_cast<SinkOperator*>(_options.operatorDag->sink());
    dassert(sink);
    while (true) {
        {
            stdx::lock_guard<Latch> lock(_mutex);
            if (_shutdown) {
                LOGV2_INFO(75896,
                           "{streamProcessorName}: exiting runLoop()",
                           "streamProcessorName"_attr = _options.streamProcessorName);
                break;
            }

            for (auto sampler : _outputSamplers) {
                sink->addOutputSampler(sampler);
            }
            _outputSamplers.clear();
        }

        bool docsFlushed = runOnce();
        if (!docsFlushed) {
            // No docs were flushed in this run, so sleep a little before starting
            // the next run.
            // TODO: add jitter
            stdx::this_thread::sleep_for(
                stdx::chrono::milliseconds(_options.sourceIdleSleepDurationMs));
        }
    }
}

}  // namespace streams
