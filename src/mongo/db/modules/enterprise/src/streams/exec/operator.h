/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mongo/util/chunked_memory_aggregator.h"
#include "mongo/util/timer.h"
#include "streams/exec/message.h"
#include "streams/exec/stream_stats.h"
#include "streams/util/metric_manager.h"

namespace streams {

struct Context;

// Size limits on StreamDataMsgs sent through the Operator dag. We try to honor these limits in a
// best-effort manner.
static constexpr int32_t kDataMsgMaxDocSize = 50000;
static constexpr int32_t kDataMsgMaxByteSize = 16 * 1024 * 1024;

/**
 * The base class of all operators in an operator dag.
 */
class Operator {
public:
    Operator(Context* context, int32_t numInputs, int32_t numOutputs);

    virtual ~Operator() = default;

    // Adds an output operator to this operator. All outputs of the operator must be added before
    // start() is called.
    // 'operInputIdx' is the input of 'oper' at which this operator is attached to it.
    void addOutput(Operator* oper, int32_t operInputIdx);

    // Starts the operator.
    // This should be called once after all operators in the dag have been attached to each other.
    void start();

    // Stops the operator.
    // This should be called once right before the dag is destroyed.
    void stop();

    /**
     * This is called when a data message and an optional control message is received
     * by this operator on its input link inputIdx.
     * inputIdx is always 0 for a single input operator.
     */
    void onDataMsg(int32_t inputIdx,
                   StreamDataMsg dataMsg,
                   boost::optional<StreamControlMsg> controlMsg = boost::none);

    /**
     * This is called when a control message is received by this operator on its input
     * link inputIdx.
     * inputIdx is always 0 for a single input operator.
     */
    void onControlMsg(int32_t inputIdx, StreamControlMsg controlMsg);

    /**
     * Returns the name of this operator.
     */
    std::string getName() const;

    OperatorStats getStats();

    /**
     * Set this operator's OperatorId. This method
     * returns the next OperatorId that is available.
     */
    void setOperatorId(OperatorId operatorId);

    /**
     * Get this operator's OperatorId.
     */
    OperatorId getOperatorId() const {
        return _operatorId;
    }

    /**
     * Operators with callback gauges should implement this method so that it registers them with
     * stream processor's executor.
     */
    virtual void registerMetrics(MetricManager* metricManager){};

protected:
    // Encapsulates metadata for an operator attached at the output of this
    // operator.
    struct OutputInfo {
        // The operator attached at the output of this operator.
        Operator* oper{nullptr};

        // The input of 'oper' at which this operator is attached to it.
        int32_t operInputIdx{0};
    };

    virtual void doStart() {}

    virtual void doStop() {}

    virtual std::string doGetName() const = 0;

    virtual void doOnDataMsg(int32_t inputIdx,
                             StreamDataMsg dataMsg,
                             boost::optional<StreamControlMsg> controlMsg) = 0;
    virtual void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) = 0;

    virtual OperatorStats doGetStats() {
        return _stats;
    }

    // Adds the given OperatorStats to _stats.
    void incOperatorStats(OperatorStats stats) {
        doIncOperatorStats(std::move(stats));
    }
    virtual void doIncOperatorStats(OperatorStats stats) {
        _stats += stats;
    }

    // Whether input byte stats should be advanced based on StreamDataMsg received in onDataMsg().
    virtual bool shouldComputeInputByteStats() const {
        return false;
    }

    /**
     * Sends a data message and an optional control message from this operator on its output
     * link outputIdx.
     * outputIdx is always 0 for a single output operator.
     */
    void sendDataMsg(int32_t outputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg = boost::none);

    /**
     * Sends a control message from this operator on its output link outputIdx.
     * outputIdx is always 0 for a single output operator.
     */
    void sendControlMsg(int32_t outputIdx, StreamControlMsg controlMsg);

    // True if the Operator is a source.
    bool isSource() {
        return doIsSource();
    }

    // True if the Operator is a sink.
    bool isSink() {
        return doIsSink();
    }

    virtual bool doIsSource() {
        return false;
    }

    virtual bool doIsSink() {
        return false;
    }

    Context* _context{nullptr};
    int32_t _numInputs{0};
    int32_t _numOutputs{0};
    OperatorId _operatorId{0};
    mongo::Timer _operatorTimer;
    OperatorStats _stats;
    // Each operator tracks its own memory usage with this handle, which propagates memory changes
    // up to the stream processor's memory aggregator and the process-wide memory aggregator.
    mongo::MemoryUsageHandle _memoryUsageHandle;

private:
    std::vector<OutputInfo> _outputs;
};

}  // namespace streams
