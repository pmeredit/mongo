#pragma once

#include <queue>

#include "streams/exec/message.h"
#include "streams/exec/operator.h"

namespace streams {

/**
 * This test-only class can act as a source or sink in an operator dag.
 * When this operator is acting as a source, you can use it to push a set of
 * documents through the operator dag.
 * When this operator is acting as a sink, you can use it to receive the result
 * documents at the end of the operator dag.
 */
class InMemorySourceSinkOperator : public Operator {
public:
    // When numInputs is 0, this operator acts as a source.
    // When numOutputs is 0, this operator acts as a sink.
    InMemorySourceSinkOperator(int32_t numInputs, int32_t numOutputs);

    /**
     * Adds a data message and an optional control message to this operator
     * that is acting as a source in the operator dag. The message will be
     * sent forward by runOnce().
     */
    void addDataMsg(StreamDataMsg dataMsg, boost::optional<StreamControlMsg> controlMsg);

    /**
     * Adds a control message to this operator that is acting as a source in the
     * operator dag. The message will be sent forward by runOnce().
     */
    void addControlMsg(StreamControlMsg controlMsg);

    /**
     * Sends forward the messages added to this operator by addDataMsg() and
     * addControlMsg(). When this method returns, _messages ends up empty.
     */
    void runOnce();

    std::queue<StreamMsgUnion>& getMessages() {
        return _messages;
    }

private:
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

    std::string doGetName() const override {
        return "InMemorySourceSinkOperator";
    }

    bool isSource() const {
        return _numInputs == 0;
    }

    bool isSink() const {
        return _numOutputs == 0;
    }

    void addDataMsgInner(StreamDataMsg dataMsg, boost::optional<StreamControlMsg> controlMsg);
    void addControlMsgInner(StreamControlMsg controlMsg);

    /**
     * When this operator is acting as a source, this field holds the messages
     * added to this operator by addDataMsg() and addControlMsg().
     * When this operator is acting as a sink, this field holds the messages
     * received via onDataMsg() and onControlMsg().
     */
    std::queue<StreamMsgUnion> _messages;
};

}  // namespace streams
