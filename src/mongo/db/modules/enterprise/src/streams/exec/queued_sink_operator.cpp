/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/queued_sink_operator.h"

#include <boost/algorithm/string.hpp>
#include <fmt/format.h>

#include "mongo/util/assert_util.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"
#include "streams/util/metric_manager.h"

namespace streams {

using namespace mongo;

namespace {

// Max size of the queue (in bytes) until `push()` starts blocking.
static constexpr int64_t kQueueMaxSizeBytes = 128 * 1024 * 1024;  // 128 MB

};  // namespace

QueuedSinkOperator::QueuedSinkOperator(Context* context, int32_t numInputs)
    : SinkOperator(context, numInputs),
      _queue(decltype(_queue)::Options{.maxQueueDepth = kQueueMaxSizeBytes}) {}

void QueuedSinkOperator::doStart() {
    stdx::lock_guard<Latch> lock(_consumerMutex);
    dassert(!_consumerThread.joinable());
    dassert(!_consumerThreadRunning);
    _consumerThread = stdx::thread([this]() { consumeLoop(); });
    _consumerThreadRunning = true;
}

void QueuedSinkOperator::doStop() {
    // This will close the queue which will make the consumer thread exit as well
    // because this will trigger a `ProducerConsumerQueueConsumed` exception in the
    // consumer thread.
    _queue.closeConsumerEnd();
    if (_consumerThread.joinable()) {
        _consumerThread.join();
    }
}

void QueuedSinkOperator::doFlush() {
    stdx::unique_lock<Latch> lock(_consumerMutex);

    dassert(!_pendingFlush);
    _pendingFlush = true;
    _queue.push(Message{.flushSignal = true});
    _flushedCv.wait(lock, [this]() -> bool { return !_consumerThreadRunning || !_pendingFlush; });

    // Make sure that an error wasn't encountered in the background consumer thread while
    // waiting for the flushed condvar to be notified.
    uassert(75386,
            str::stream() << "unable to flush queued sink operator with error: "
                          << _consumerError.value_or("unknown"),
            !_consumerError && !_pendingFlush);
}

boost::optional<std::string> QueuedSinkOperator::doGetError() {
    stdx::lock_guard<Latch> lock(_consumerMutex);
    return _consumerError;
}

void QueuedSinkOperator::registerMetrics(MetricManager* metricManager) {
    _queueSize = metricManager->registerCallbackGauge(
        fmt::format("{}_queue_bytesize", boost::algorithm::to_lower_copy(getName())),
        /* description */ "Total bytes currently buffered in the queue",
        /*labels*/ getDefaultMetricLabels(_context),
        [this]() { return _queue.getStats().queueDepth; });
}

OperatorStats QueuedSinkOperator::doGetStats() {
    OperatorStats stats;
    {
        stdx::lock_guard<Latch> lock(_consumerMutex);
        std::swap(_consumerStats, stats);
    }

    incOperatorStats(stats);
    return _stats;
}

void QueuedSinkOperator::doSinkOnDataMsg(int32_t inputIdx,
                                         StreamDataMsg dataMsg,
                                         boost::optional<StreamControlMsg> controlMsg) {
    _queue.push(Message{.data = std::move(dataMsg)});
}

void QueuedSinkOperator::consumeLoop() {
    bool done{false};
    boost::optional<std::string> error;

    while (!done) {
        try {
            auto msg = _queue.pop();
            if (msg.flushSignal) {
                stdx::lock_guard<Latch> lock(_consumerMutex);
                _pendingFlush = false;
                _flushedCv.notify_all();
            } else {
                auto stats = processDataMsg(std::move(*msg.data));

                stdx::lock_guard<Latch> lock(_consumerMutex);
                _consumerStats += stats;
            }
        } catch (const ExceptionFor<ErrorCodes::ProducerConsumerQueueEndClosed>&) {
            // Closed naturally from `stop()`.
            done = true;
        } catch (const DBException& e) {
            // TODO Figure out a way to transfer the inner error code directly. The toString()
            // result has the error code information in the form of "Location1234500: error
            // message".
            error = e.toString();
            done = true;
        } catch (const std::exception& e) {
            error = e.what();
            done = true;
        }
    }

    // Wake up the executor thread if its waiting on a flush. If we're exiting the consume
    // loop because of an exception, then the flush in the executor thread will fail after
    // it receives the flushed condvar signal.
    stdx::lock_guard<Latch> lock(_consumerMutex);
    _consumerError = std::move(error);
    _consumerThreadRunning = false;
    _flushedCv.notify_all();
}

};  // namespace streams
