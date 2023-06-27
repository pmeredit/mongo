#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "streams/exec/message.h"
#include "streams/exec/stream_stats.h"

namespace streams {

struct Context;

/**
 * Used to identify operators in checkpoint data.
 * Each Operator in the DAG receives a unique OperatorId.
 * This includes Operators in a window's inner pipeline.
 */
using OperatorId = int32_t;

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

    OperatorStats getStats() const {
        return _stats;
    }

    /**
     * Set this operator's OperatorId. This method
     * returns the next OperatorId that is available.
     */
    void setOperatorId(OperatorId operatorId);

    /**
     * Returns the number of inner operators this Operator has.
     * For the base Operator, this is zero. WindowOperator has more than zero.
     * TODO(SERVER-78479): Use visitor pattern.
     */
    virtual int32_t getNumInnerOperators() const {
        return 0;
    }

    /**
     * Get this operator's OperatorId.
     */
    OperatorId getOperatorId() const {
        return _operatorId;
    }

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

    Context* _context{nullptr};
    int32_t _numInputs{0};
    int32_t _numOutputs{0};
    OperatorId _operatorId{0};
    OperatorStats _stats;

private:
    std::vector<OutputInfo> _outputs;
};

}  // namespace streams
