/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <deque>
#include <memory>
#include <string>

#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/message.h"
#include "streams/exec/producer_consumer_queue.h"
#include "streams/exec/queued_sink_operator.h"
#include "streams/exec/sink_operator.h"
#include "streams/exec/stream_stats.h"
#include "streams/util/metrics.h"

namespace streams {

struct Context;
class MetricManager;

// Base class for sink operators that process input documents asynchronously.
class QueuedSinkOperator : public SinkOperator {
public:
    QueuedSinkOperator(Context* context, int32_t numInputs);
    ~QueuedSinkOperator() override {
        _queue.shutDown();
    }

    // Registers metrics for this queued sink operator.
    void registerMetrics(MetricManager* metricManager) override;

protected:
    // Cost function for the queue so that we limit the max queue size based on the
    // byte size of the documents rather than having the same weight for each document.
    static ProducerConsumerQueue<StreamDataMsg>::Cost queueCostFunc(const StreamDataMsg& msg) {
        return {.entrySize = static_cast<int64_t>(msg.docs.size()),
                .entrySizeBytes = msg.getByteSize()};
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
    // Background thread that processes documents from `_queue`.
    mongo::stdx::thread _consumerThread;
    mutable mongo::stdx::mutex _mutex;

    // Status of the the background consumer thread, protected by `_mutex`.
    ConnectionStatus _consumerStatus{ConnectionStatus::kConnecting};

    // Whether the background consumer thread is currently running.
    bool _consumerThreadRunning{false};

    std::shared_ptr<IntGauge> _queueSizeGauge;
    std::shared_ptr<IntGauge> _queueByteSizeGauge;

    // Stats tracked by the consumer thread. Write and read access to these stats must be
    // protected by `_mutex`. This will be merged with the root level `_stats`
    // when `doGetStats()` is called. Protected by `_mutex`.
    OperatorStats _consumerStats;

    ProducerConsumerQueue<StreamDataMsg> _queue;
};

}  // namespace streams
