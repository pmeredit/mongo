#pragma once

#include <queue>

#include "mongo/platform/mutex.h"
#include "streams/exec/message.h"
#include "streams/exec/sink_operator.h"

namespace streams {

/**
 * This test-only class can act as a sink in an operator dag.
 * You can use it to receive the result documents at the end of the operator dag.
 * This class is thread-safe.
 */
class InMemorySinkOperator : public SinkOperator {
public:
    InMemorySinkOperator(Context* context, int32_t numInputs);

    std::queue<StreamMsgUnion> getMessages();

private:
    friend class OutputSamplerTest;

    void doSinkOnDataMsg(int32_t inputIdx,
                         StreamDataMsg dataMsg,
                         boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

    std::string doGetName() const override {
        return "InMemorySinkOperator";
    }

    void addDataMsgInner(StreamDataMsg dataMsg, boost::optional<StreamControlMsg> controlMsg);
    void addControlMsgInner(StreamControlMsg controlMsg);

    // Guards _messages.
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("InMemorySinkOperator::mutex");
    /**
     * This field holds the messages received via onDataMsg() and onControlMsg().
     */
    std::queue<StreamMsgUnion> _messages;
};

}  // namespace streams
