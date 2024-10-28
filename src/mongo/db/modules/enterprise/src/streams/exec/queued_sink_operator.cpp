/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/queued_sink_operator.h"

#include <boost/algorithm/string.hpp>
#include <fmt/format.h>

#include "mongo/base/error_codes.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"
#include "streams/util/exception.h"
#include "streams/util/metric_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

MONGO_FAIL_POINT_DEFINE(queuedSinkStopProcessingData);

QueuedSinkOperator::QueuedSinkOperator(Context* context, int32_t numInputs)
    : SinkOperator(context, numInputs),
      _queue(decltype(_queue)::Options{.costFunc = queueCostFunc,
                                       .maxQueueSize = getMaxSinkQueueSize(_context->featureFlags),
                                       .maxQueueSizeBytes =
                                           getMaxSinkQueueSizeBytes(_context->featureFlags)}) {}

void QueuedSinkOperator::doStart() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
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
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            if (status.isOK()) {
                _consumerStatus = ConnectionStatus{ConnectionStatus::kConnected};
            } else {
                _consumerStatus = ConnectionStatus{ConnectionStatus::kError, std::move(status)};
                _consumerThreadRunning = false;
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
    // This will close the queue which will make the consumer thread to exit as well.
    _queue.shutDown();
    if (_consumerThread.joinable()) {
        _consumerThread.join();
    }
}

void QueuedSinkOperator::doFlush() {
    // Push an empty StreamDataMsg as a flush signal to the queue.
    auto status = _queue.push(StreamDataMsg{});
    tassert(status.code(),
            str::stream() << "Push to queue failed with error - " << status.reason(),
            status.isOK());
    status = _queue.waitForEmpty();
    tassert(status.code(),
            str::stream() << "Flush failed with error - " << status.reason(),
            status.isOK());

    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        // Make sure that an error wasn't encountered in the background consumer thread while
        // waiting for the flush to finish.
        _consumerStatus.throwIfNotConnected();
    }
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
        stdx::lock_guard<stdx::mutex> lock(_mutex);
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
    auto status = _queue.push(std::move(dataMsg));
    tassert(status.code(),
            str::stream() << "Push to queue failed with error - " << status.reason(),
            status.isOK());
}

ConnectionStatus QueuedSinkOperator::doGetConnectionStatus() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _consumerStatus;
}

void QueuedSinkOperator::consumeLoop() {
    bool done{false};
    SPStatus status;

    while (!done) {
        try {
            // If queuedSinkStopProcessingData fail point is ON, the consumer thread will not
            // process the Message queue. Can be used to accumulate data in the Message queue.
            if (MONGO_unlikely(queuedSinkStopProcessingData.shouldFail())) {
                sleep(2);
                continue;
            }

            auto msg = _queue.pop();
            if (!msg) {
                // No message found in the queue, queue shut down.
                break;
            }

            // Check if it's an empty message used as a flush signal.
            if ((*msg).docs.empty()) {
                continue;
            }

            _queueSizeGauge->incBy(-1 * int64_t((*msg).docs.size()));
            _queueByteSizeGauge->incBy(-1 * (*msg).getByteSize());
            auto stats = processDataMsg(std::move(*msg));

            stdx::lock_guard<stdx::mutex> lock(_mutex);
            _consumerStats += stats;
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
    // This will cause any thread calling push to the message queue to throw an exception.
    _queue.shutDown();

    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        // Wake up the executor thread if its waiting on a flush. If we're exiting the consume
        // loop because of an exception, then the flush in the executor thread will fail after
        // it receives the flushed condvar signal.
        if (!status.isOK()) {
            _consumerStatus = ConnectionStatus{ConnectionStatus::kError, std::move(status)};
        }
        _consumerThreadRunning = false;
    }
}

};  // namespace streams
