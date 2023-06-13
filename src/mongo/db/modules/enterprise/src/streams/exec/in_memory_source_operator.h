#pragma once

#include <queue>

#include "mongo/platform/mutex.h"
#include "streams/exec/message.h"
#include "streams/exec/source_operator.h"

namespace streams {

/**
 * This test-only class can act as a source in an operator dag.
 * You can use it to push a set of documents through the operator dag.
 * This class is thread-safe.
 */
class InMemorySourceOperator : public SourceOperator {
public:
    InMemorySourceOperator(Context* context, int32_t numOutputs);

    /**
     * Adds a data message and an optional control message to this operator.
     * The message will be sent forward by runOnce().
     */
    void addDataMsg(StreamDataMsg dataMsg,
                    boost::optional<StreamControlMsg> controlMsg = boost::none);

    /**
     * Adds a control message to this operator.
     * The message will be sent forward by runOnce().
     */
    void addControlMsg(StreamControlMsg controlMsg);

    std::queue<StreamMsgUnion> getMessages();

private:
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override {
        MONGO_UNREACHABLE;
    }
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override {
        MONGO_UNREACHABLE;
    }

    // Sends forward the messages added to this operator by addDataMsg() and
    // addControlMsg(). When this method returns, _messages ends up empty.
    int32_t doRunOnce() override;

    std::string doGetName() const override {
        return "InMemorySourceOperator";
    }

    void addDataMsgInner(StreamDataMsg dataMsg, boost::optional<StreamControlMsg> controlMsg);
    void addControlMsgInner(StreamControlMsg controlMsg);

    // Guards _messages.
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("InMemorySourceOperator::mutex");
    /**
     * This field holds the messages added to this operator by addDataMsg() and addControlMsg().
     */
    std::queue<StreamMsgUnion> _messages;
};

}  // namespace streams
