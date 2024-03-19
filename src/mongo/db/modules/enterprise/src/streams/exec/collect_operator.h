#pragma once

#include <deque>

#include "mongo/platform/mutex.h"
#include "streams/exec/message.h"
#include "streams/exec/sink_operator.h"

namespace streams {

/**
 * This SinkOperator simply collects the messages received from the operator dag in memory.
 */
class CollectOperator : public SinkOperator {
public:
    CollectOperator(Context* context, int32_t numInputs);

    ~CollectOperator() override = default;

    std::deque<StreamMsgUnion> getMessages() {
        return doGetMessages();
    }

protected:
    void doSinkOnDataMsg(int32_t inputIdx,
                         StreamDataMsg dataMsg,
                         boost::optional<StreamControlMsg> controlMsg) override;
    void doSinkOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

    std::string doGetName() const override {
        return "CollectOperator";
    }

    OperatorStats doGetStats() override {
        _stats.memoryUsageBytes = _memoryUsageHandle.getCurrentMemoryUsageBytes();
        return _stats;
    }

    virtual std::deque<StreamMsgUnion> doGetMessages();

private:
    // This field holds the messages received via onDataMsg() and onControlMsg().
    std::deque<StreamMsgUnion> _messages;
};

}  // namespace streams
