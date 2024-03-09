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

QueuedSinkOperator::QueuedSinkOperator(Context* context, int32_t numInputs)
    : SinkOperator(context, numInputs),
      _queue(decltype(_queue)::Options{.maxQueueDepth = kQueueMaxSizeBytes}) {}

void QueuedSinkOperator::doStart() {
    stdx::lock_guard<Latch> lock(_consumerMutex);
    dassert(!_consumerThread.joinable());
    dassert(!_consumerThreadRunning);
    _consumerThread = stdx::thread([this]() {
        // Validate the connection with the target.
        SPStatus status{Status::OK()};
        try {
            validateConnection();
        } catch (const SPException& e) {
            LOGV2_INFO(8520399,
                       "SPException occured in QueuedSinkOperator validateConnection",
                       "code"_attr = e.code(),
                       "reason"_attr = e.reason(),
                       "unsafeErrorMessage"_attr = e.unsafeReason());
            status = e.toStatus();
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
            status = SPStatus{{ErrorCodes::UnknownError, "Unkown error occured in sink operator."},
                              e.what()};
        }

        // If validateConnection succeeded, enter a kConnected state.
        // Otherwise enter an error state and return.
        {
            stdx::lock_guard<Latch> lock(_consumerMutex);
            if (status.isOK()) {
                _consumerStatus = ConnectionStatus{ConnectionStatus::kConnected};
            } else {
                _consumerStatus = ConnectionStatus{ConnectionStatus::kError, std::move(status)};
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
    spassert(_consumerStatus.error, _consumerStatus.isConnected());
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
    SPStatus status;

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
            status = SPStatus(e.toStatus());
            done = true;
        } catch (const std::exception& e) {
            status = {{mongo::ErrorCodes::UnknownError, "An unknown error occured in " + getName()},
                      e.what()};
            done = true;
        }
    }

    // Wake up the executor thread if its waiting on a flush. If we're exiting the consume
    // loop because of an exception, then the flush in the executor thread will fail after
    // it receives the flushed condvar signal.
    stdx::lock_guard<Latch> lock(_consumerMutex);
    if (!status.isOK()) {
        _consumerStatus = ConnectionStatus{ConnectionStatus::kError, std::move(status)};
    }
    _consumerThreadRunning = false;
    _flushedCv.notify_all();
}

};  // namespace streams
