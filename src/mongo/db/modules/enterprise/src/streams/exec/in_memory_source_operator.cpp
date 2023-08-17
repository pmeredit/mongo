/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#include "streams/exec/in_memory_source_operator.h"

namespace streams {

using namespace mongo;

InMemorySourceOperator::InMemorySourceOperator(Context* context, int32_t numOutputs)
    : SourceOperator(context, numOutputs) {}

void InMemorySourceOperator::addDataMsg(StreamDataMsg dataMsg,
                                        boost::optional<StreamControlMsg> controlMsg) {
    addDataMsgInner(std::move(dataMsg), std::move(controlMsg));
}

void InMemorySourceOperator::addDataMsgInner(StreamDataMsg dataMsg,
                                             boost::optional<StreamControlMsg> controlMsg) {
    StreamMsgUnion msg;
    msg.dataMsg = std::move(dataMsg);
    msg.controlMsg = std::move(controlMsg);

    stdx::lock_guard<Latch> lock(_mutex);
    _messages.push(std::move(msg));
}

void InMemorySourceOperator::addControlMsg(StreamControlMsg controlMsg) {
    addControlMsgInner(std::move(controlMsg));
}

void InMemorySourceOperator::addControlMsgInner(StreamControlMsg controlMsg) {
    StreamMsgUnion msg;
    msg.controlMsg = std::move(controlMsg);

    stdx::lock_guard<Latch> lock(_mutex);
    _messages.push(std::move(msg));
}

int64_t InMemorySourceOperator::doRunOnce() {
    int64_t numDocsFlushed{0};
    stdx::lock_guard<Latch> lock(_mutex);
    while (!_messages.empty()) {
        StreamMsgUnion msg = std::move(_messages.front());
        _messages.pop();

        if (msg.dataMsg) {
            int64_t numInputBytes{0};
            for (const auto& doc : msg.dataMsg->docs) {
                numInputBytes += doc.doc.getCurrentApproximateSize();
            }
            incOperatorStats(OperatorStats{.numInputDocs = int64_t(msg.dataMsg->docs.size()),
                                           .numInputBytes = numInputBytes});

            numDocsFlushed += msg.dataMsg->docs.size();
            sendDataMsg(/*outputIdx*/ 0, std::move(msg.dataMsg.get()), std::move(msg.controlMsg));
        } else {
            sendControlMsg(/*outputIdx*/ 0, std::move(msg.controlMsg.get()));
        }
    }
    return numDocsFlushed;
}

std::queue<StreamMsgUnion> InMemorySourceOperator::getMessages() {
    stdx::lock_guard<Latch> lock(_mutex);
    auto messages = std::move(_messages);
    _messages = std::queue<StreamMsgUnion>();
    return messages;
}

}  // namespace streams
