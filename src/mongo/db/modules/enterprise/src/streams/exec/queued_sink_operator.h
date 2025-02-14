/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/algorithm/string/case_conv.hpp>
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

// The SinkWriter interface encapsulates sink-specific writing logic, i.e. for
// $merge to AtlasDBs or timeseries $emit. A QueuedSinkOperator manages 1 or more
// background writer threads. Each has has a SinkWriter instance.
class SinkWriter {
public:
    SinkWriter(Context* context, SinkOperator* sinkOperator)
        : _context(context), _sinkOperator(sinkOperator) {}

    virtual ~SinkWriter() {}

    // Process a batch of messages.
    virtual OperatorStats processDataMsg(StreamDataMsg dataMsg) = 0;

    // Validate the connection, throw exception if it's invalid.
    virtual void connect() = 0;

    // Return the docs partition. QueuedSinkOperator uses this to partition messages to different
    // writer threads. The partition implementation can modify the doc if needed.
    virtual size_t partition(StreamDocument& doc) {
        MONGO_UNIMPLEMENTED;
    }

    // Return the operator name associated with this SinkWriter.
    std::string getName() const {
        return _sinkOperator->getName();
    }

    // Register metrics for this writer.
    void registerMetrics(MetricManager* metricManager, const MetricManager::LabelsVec& labels) {
        _writeLatencyMs = metricManager->registerHistogram(
            fmt::format("{}_write_latency_ms", boost::algorithm::to_lower_copy(getName())),
            /* description */ "Latency for sync batch writes to the sink.",
            /* labels */ labels,
            makeExponentialDurationBuckets(
                /* start */ mongo::stdx::chrono::milliseconds(5), /* factor */ 5, /* count */ 6));
    }

    // Send output to samplers.
    void sendOutputToSamplers(const StreamDataMsg& dataMsg) {
        _sinkOperator->sendOutputToSamplers(dataMsg);
    }

    // Returns true if samplers exist.
    bool samplersExist() const {
        return _sinkOperator->samplersExist();
    }

protected:
    // Write latency (in milliseconds) recorded from the subclass.
    std::shared_ptr<Histogram> _writeLatencyMs;

    Context* _context{nullptr};
    SinkOperator* _sinkOperator{nullptr};
};

// QueuedSinkOperator is the base class for sink operators that process input documents in
// background threads. The QueuedSinkOperator manages one or more WriterThreads and routes messages
// to those threads. Each WriterThread has it's own message queue and SinkWriter instance.
class QueuedSinkOperator : public SinkOperator {
public:
    QueuedSinkOperator(Context* context, int32_t numInputs, int32_t parallelism);

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

    // Starts up the background consumer thread.
    void doStart() override;

    // Stops the background consumer thread.
    void doStop() override;

    ConnectionStatus doGetConnectionStatus() override;

    // Make a SinkWriter instance.
    virtual std::unique_ptr<SinkWriter> makeWriter() = 0;

    // Connection type of this QueuedSinkOperator.
    virtual mongo::ConnectionTypeEnum getConnectionType() const = 0;

    // Ensure that all in-flight data messages in the queue are processed before returning.
    void doFlush() override;

    // Adds the data message into the work queue.
    void doSinkOnDataMsg(int32_t inputIdx,
                         StreamDataMsg dataMsg,
                         boost::optional<StreamControlMsg> controlMsg) override;

    // Merges `_consumerStats` into `_stats` before returning.
    OperatorStats doGetStats() override;

private:
    friend class MergeOperatorTest;

    struct WriterThread {
        // Handles sink specific writing logic.
        std::unique_ptr<SinkWriter> writer;

        // All messages are processed asynchronously by the `_consumerThread`.
        std::unique_ptr<mongo::SingleProducerSingleConsumerQueue<Message, QueueCostFunc>> queue;

        // Processor context.
        Context* context{nullptr};

        // Background thread that processes documents from `_queue`.
        mongo::stdx::thread consumerThread;
        mutable mongo::stdx::mutex consumerMutex;

        // Status of the the background consumer thread, protected by `_consumerMutex`.
        ConnectionStatus consumerStatus{ConnectionStatus::kConnecting};

        // Whether the background consumer thread is currently running.
        bool consumerThreadRunning{false};

        // When flush is called on the sink operator, we need to wait until the work queue is fully
        // drained and finished processing by the background consumer thread. The `flush()` call
        // will wait on this condvar, which will be notified by the background consumer thread after
        // all in-flight messages have been processed. Protected by `_consumerMutex`.
        mongo::stdx::condition_variable flushedCv;
        bool pendingFlush{false};

        // Queue size metrics for this WriterThread.
        std::shared_ptr<IntGauge> queueSizeGauge;
        std::shared_ptr<IntGauge> queueByteSizeGauge;

        // Stats tracked by the consumer thread. Write and read access to these stats must be
        // protected by `_consumerMutex`. This will be merged with the root level `_stats`
        // when `doGetStats()` is called. Protected by `_consumerMutex`.
        OperatorStats consumerStats;

        // Start the IO thread.
        void start();

        // Stop the IO thread.
        void stop();

        // Flush the IO thread.
        void flush();

        // Background consumer thread loop.
        void consumeLoop();

        // Register metrics.
        void registerMetrics(MetricManager* metricManager, int threadId);
    };

    // Create a new WriterThread.
    std::unique_ptr<WriterThread> makeThread(std::unique_ptr<SinkWriter> writer);

    // Number of WriterThreads to use.
    int _parallelism{1};

    // A vector of WriterThread instances. A WriterThread handles the logic to
    // write messages to the target.
    std::vector<std::unique_ptr<WriterThread>> _threads;

    // MetricManager used by this Operator and its WriterThreads.
    MetricManager* _metricManager{nullptr};
};

}  // namespace streams
