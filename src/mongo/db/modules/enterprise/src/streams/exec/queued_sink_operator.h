/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/base/status.h"
#include "streams/exec/connection_status.h"
#include <boost/optional.hpp>
#include <memory>
#include <string>

#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/producer_consumer_queue.h"
#include "streams/exec/message.h"
#include "streams/exec/sink_operator.h"
#include "streams/exec/stream_stats.h"
#include "streams/util/metrics.h"

namespace streams {

static constexpr int32_t kSinkDataMsgMaxDocSize = 50000;
static constexpr int32_t kSinkDataMsgMaxByteSize = 15 * 1024 * 1024;

struct Context;
class MetricManager;

// Base class for sink operators that process input documents asynchronously.
class QueuedSinkOperator : public SinkOperator {
public:
    QueuedSinkOperator(Context* context, int32_t numInputs);

    // Registers metrics for this queued sink operator.
    void registerMetrics(MetricManager* metricManager) override;

protected:
    // Single queue entry, only either `data` or `flushSignal` will be set. The
    // `flushSignal` is only used internally on `flush()` to ensure that the
    // consumer thread signals back to the caller thread that the queue has been
    // flushed to mongodb before commiting the checkpointing.
    struct Message {
        // Document received from `doSinkOnDataMsg`, this is only marked as optional for
        // the case where `flushSignal` is set, which is used internally for `flush()`.
        boost::optional<StreamDataMsg> data;

        // Used by checkpointing to ensure that the queue is drained and that the inflight
        // document batch has been written out to mongodb.
        bool flushSignal{false};
    };

    // Cost function for the queue so that we limit the max queue size based on the
    // byte size of the documents rather than having the same weight for each document.
    struct QueueCostFunc {
        size_t operator()(const Message& msg) const {
            if (!msg.data) {
                // This is only the case for internal `flush()` messages.
                return 1;
            }

            auto size = msg.data->getByteSize();
            if (size > maxSizeBytes) {
                // ProducerConsumerQueue will throw ProducerConsumerQueueBatchTooLarge if a single
                // item is larger than the max queue size. We want to allow a single large
                // StreamDataMsg in the queue.
                return maxSizeBytes;
            }
            return size;
        }

        int64_t maxSizeBytes{0};
    };

    // Called from the background consumer thread as it pops data messages from the queue.
    virtual OperatorStats processDataMsg(StreamDataMsg dataMsg) = 0;

    // Called first in the background consumer thread. The operator should make any network requests
    // required to validate its connections, and throw a DBException if there is an error.
    virtual void validateConnection() {}

    // Starts up the background consumer thread.
    void doStart() override;

    // Stops the background consumer thread.
    void doStop() override;

    ConnectionStatus doGetConnectionStatus() override;

    // Ensure that all in-flight data messages in the queue are processed before returning.
    void doFlush() override;

    // Adds the data message into the work queue.
    void doSinkOnDataMsg(int32_t inputIdx,
                         StreamDataMsg dataMsg,
                         boost::optional<StreamControlMsg> controlMsg) override;

    // Merges `_consumerStats` into `_stats` before returning.
    OperatorStats doGetStats() override;

    // Background consumer thread loop.
    void consumeLoop();

    // Write latency (in milliseconds) recorded from the subclass.
    std::shared_ptr<Histogram> _writeLatencyMs;

private:
    // All messages are processed asynchronously by the `_consumerThread`.
    mongo::SingleProducerSingleConsumerQueue<Message, QueueCostFunc> _queue;

    // Background thread that processes documents from `_queue`.
    mongo::stdx::thread _consumerThread;
    mutable mongo::stdx::mutex _consumerMutex;

    // Status of the the background consumer thread, protected by `_consumerMutex`.
    ConnectionStatus _consumerStatus{ConnectionStatus::kConnecting};

    // Whether the background consumer thread is currently running.
    bool _consumerThreadRunning{false};

    // When flush is called on the sink operator, we need to wait until the work queue is fully
    // drained and finished processing by the background consumer thread. The `flush()` call will
    // wait on this condvar, which will be notified by the background consumer thread after all
    // in-flight messages have been processed. Protected by `_consumerMutex`.
    mongo::stdx::condition_variable _flushedCv;
    bool _pendingFlush{false};

    std::shared_ptr<IntGauge> _queueSizeGauge;
    std::shared_ptr<IntGauge> _queueByteSizeGauge;

    // Stats tracked by the consumer thread. Write and read access to these stats must be
    // protected by `_consumerMutex`. This will be merged with the root level `_stats`
    // when `doGetStats()` is called. Protected by `_consumerMutex`.
    OperatorStats _consumerStats;
};

}  // namespace streams
