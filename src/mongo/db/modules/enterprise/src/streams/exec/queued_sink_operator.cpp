/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/queued_sink_operator.h"

#include "mongo/base/error_codes.h"
#include "mongo/logv2/log.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/stream_processor_feature_flags.h"
#include "streams/util/exception.h"
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
      _queue(decltype(_queue)::Options{
          .maxQueueDepth = static_cast<size_t>(getMaxQueueSizeBytes(_context->featureFlags)),
          .costFunc =
              QueueCostFunc{.maxSizeBytes = getMaxQueueSizeBytes(_context->featureFlags)}}) {}

void QueuedSinkOperator::doStart() {
    stdx::lock_guard<stdx::mutex> lock(_consumerMutex);
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
                       "context"_attr = _context,
                       "code"_attr = e.code(),
                       "reason"_attr = e.reason(),
                       "unsafeErrorMessage"_attr = e.unsafeReason());
            status = e.toStatus();
        } catch (const DBException& e) {
            LOGV2_INFO(8520301,
                       "Exception occured in QueuedSinkOperator validateConnection",
                       "context"_attr = _context,
                       "code"_attr = e.code(),
                       "reason"_attr = e.reason());
            status = e.toStatus();
        } catch (const std::exception& e) {
            LOGV2_WARNING(
                8520300,
                "Unexpected std::exception occured in QueuedSinkOperator validateConnection",
                "context"_attr = _context,
                "exception"_attr = e.what());
            status = SPStatus{{ErrorCodes::UnknownError, "Unkown error occured in sink operator."},
                              e.what()};
        }

        // If validateConnection succeeded, enter a kConnected state.
        // Otherwise enter an error state and return.
        {
            stdx::lock_guard<stdx::mutex> lock(_consumerMutex);
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
    stdx::unique_lock<stdx::mutex> lock(_consumerMutex);

    dassert(!_pendingFlush);
    _pendingFlush = true;
    _queue.push(Message{.flushSignal = true});
    _flushedCv.wait(lock, [this]() -> bool { return !_consumerThreadRunning || !_pendingFlush; });

    // Make sure that an error wasn't encountered in the background consumer thread while
    // waiting for the flushed condvar to be notified.
    _consumerStatus.throwIfNotConnected();
    uassert(75386, str::stream() << "Unable to flush queued sink operator", !_pendingFlush);
}

void QueuedSinkOperator::registerMetrics(MetricManager* metricManager) {
    _queueSizeGauge = metricManager->registerIntGauge(
        "sink_operator_queue_size",
        /* description */ "Total docs currently buffered in the queue",
        /*labels*/ getDefaultMetricLabels(_context));
    _queueByteSizeGauge = metricManager->registerIntGauge(
        "sink_operator_queue_bytesize",
        /* description */ "Total bytes currently buffered in the queue",
        /*labels*/ getDefaultMetricLabels(_context));
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
        stdx::lock_guard<stdx::mutex> lock(_consumerMutex);
        std::swap(_consumerStats, stats);
    }

    incOperatorStats(stats);
    return _stats;
}

void QueuedSinkOperator::doSinkOnDataMsg(int32_t inputIdx,
                                         StreamDataMsg dataMsg,
                                         boost::optional<StreamControlMsg> controlMsg) {
    _queueSizeGauge->incBy(int64_t(dataMsg.docs.size()));
    _queueByteSizeGauge->incBy(dataMsg.getByteSize());
    _queue.push(Message{.data = std::move(dataMsg)});
}

ConnectionStatus QueuedSinkOperator::doGetConnectionStatus() {
    stdx::lock_guard<stdx::mutex> lock(_consumerMutex);
    return _consumerStatus;
}

void QueuedSinkOperator::consumeLoop() {
    bool done{false};
    SPStatus status;

    while (!done) {
        try {
            auto msg = _queue.pop();
            if (msg.flushSignal) {
                stdx::lock_guard<stdx::mutex> lock(_consumerMutex);
                _pendingFlush = false;
                _flushedCv.notify_all();
            } else {
                _queueSizeGauge->incBy(-1 * int64_t(msg.data->docs.size()));
                _queueByteSizeGauge->incBy(-1 * msg.data->getByteSize());
                auto stats = processDataMsg(std::move(*msg.data));

                stdx::lock_guard<stdx::mutex> lock(_consumerMutex);
                _consumerStats += stats;
            }
        } catch (const ExceptionFor<ErrorCodes::ProducerConsumerQueueEndClosed>&) {
            // Closed naturally from `stop()`.
            done = true;
        } catch (const SPException& e) {
            status = e.toStatus();
            done = true;
        } catch (const DBException& e) {
            status = SPStatus(e.toStatus());
            done = true;
        } catch (const std::exception& e) {
            LOGV2_WARNING(8748301,
                          "Unexpected std::exception in queued_sink_operator",
                          "context"_attr = _context,
                          "operatorName"_attr = getName(),
                          "exception"_attr = e.what());
            status = {{mongo::ErrorCodes::UnknownError, "An unknown error occured in " + getName()},
                      e.what()};
            done = true;
        }
    }

    // This will cause any thread calling _queue.push to throw an exception.
    _queue.closeConsumerEnd();

    // Wake up the executor thread if its waiting on a flush. If we're exiting the consume
    // loop because of an exception, then the flush in the executor thread will fail after
    // it receives the flushed condvar signal.
    stdx::lock_guard<stdx::mutex> lock(_consumerMutex);
    if (!status.isOK()) {
        _consumerStatus = ConnectionStatus{ConnectionStatus::kError, std::move(status)};
    }
    _consumerThreadRunning = false;
    _flushedCv.notify_all();
}

};  // namespace streams
