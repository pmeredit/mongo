/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/queued_sink_operator.h"

#include "mongo/base/error_codes.h"
#include "streams/exec/connection_status.h"
#include <boost/algorithm/string.hpp>
#include <fmt/format.h>

#include "mongo/util/assert_util.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"
#include "streams/util/metric_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

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
    _consumerThread = stdx::thread([this]() {
        // Validate the connection with the target.
        auto status = Status::OK();
        try {
            validateConnection();
        } catch (const DBException& e) {
            LOGV2_INFO(8520301,
                       "Exception occured in QueuedSinkOperator validateConnection",
                       "code"_attr = e.code(),
                       "reason"_attr = e.reason());
            status = e.toStatus();
        } catch (const std::exception& e) {
            LOGV2_WARNING(
                8520300,
                "Unexpected std::exception occured in QueuedSinkOperator validateConnection",
                "exception"_attr = e.what());
            status = Status{ErrorCodes::UnknownError, "Unkown error occured in sink operator."};
        }

        // If validateConnection succeeded, enter a kConnected state.
        // Otherwise enter an error state and return.
        {
            stdx::lock_guard<Latch> lock(_consumerMutex);
            if (status.isOK()) {
                _consumerStatus = ConnectionStatus{ConnectionStatus::kConnected};
            } else {
                _consumerStatus = ConnectionStatus{
                    ConnectionStatus::kError, status.code(), std::move(status.reason())};
                _consumerThreadRunning = false;
                _flushedCv.notify_all();
                // Return early in error.
                return;
            }
        }

        // Start consuming messages.
        consumeLoop();
    });
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
    uassert(_consumerStatus.errorCode, _consumerStatus.errorReason, _consumerStatus.isConnected());
    uassert(75386, str::stream() << "Unable to flush queued sink operator", !_pendingFlush);
}

void QueuedSinkOperator::registerMetrics(MetricManager* metricManager) {
    _queueSize = metricManager->registerCallbackGauge(
        fmt::format("{}_queue_bytesize", boost::algorithm::to_lower_copy(getName())),
        /* description */ "Total bytes currently buffered in the queue",
        /*labels*/ getDefaultMetricLabels(_context),
        [this]() { return _queue.getStats().queueDepth; });
    _writeLatencyMs = metricManager->registerHistogram(
        fmt::format("{}_write_latency_ms", boost::algorithm::to_lower_copy(getName())),
        /* description */ "Latency for sync batch writes to the sink.",
        /* labels */ getDefaultMetricLabels(_context),
        /* buckets */
        makeExponentialDurationBuckets(
            /* start */ stdx::chrono::milliseconds(5), /* factor */ 5, /* count */ 6));
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

ConnectionStatus QueuedSinkOperator::doGetConnectionStatus() {
    stdx::lock_guard<Latch> lock(_consumerMutex);
    return _consumerStatus;
}

void QueuedSinkOperator::consumeLoop() {
    bool done{false};
    boost::optional<std::string> error;
    ErrorCodes::Error errorCode = mongo::ErrorCodes::OK;

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
            errorCode = e.code();
            done = true;
        } catch (const std::exception& e) {
            error = e.what();
            errorCode = mongo::ErrorCodes::UnknownError;
            done = true;
        }
    }

    // Wake up the executor thread if its waiting on a flush. If we're exiting the consume
    // loop because of an exception, then the flush in the executor thread will fail after
    // it receives the flushed condvar signal.
    stdx::lock_guard<Latch> lock(_consumerMutex);
    if (errorCode != ErrorCodes::OK) {
        invariant(error);
        _consumerStatus = ConnectionStatus{ConnectionStatus::kError, errorCode, error.get()};
    }
    _consumerThreadRunning = false;
    _flushedCv.notify_all();
}

};  // namespace streams
