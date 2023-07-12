#pragma once

#include <queue>

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

    virtual ~CollectOperator() = default;

    std::queue<StreamMsgUnion> getMessages() {
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

    virtual std::queue<StreamMsgUnion> doGetMessages();

private:
    // This field holds the messages received via onDataMsg() and onControlMsg().
    std::queue<StreamMsgUnion> _messages;
};

}  // namespace streams
