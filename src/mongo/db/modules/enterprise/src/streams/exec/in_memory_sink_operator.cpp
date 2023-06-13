/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#include "streams/exec/in_memory_sink_operator.h"

namespace streams {

using namespace mongo;

InMemorySinkOperator::InMemorySinkOperator(Context* context, int32_t numInputs)
    : SinkOperator(context, numInputs) {
    dassert(numInputs != 0);
}

void InMemorySinkOperator::addDataMsgInner(StreamDataMsg dataMsg,
                                           boost::optional<StreamControlMsg> controlMsg) {
    StreamMsgUnion msg;
    msg.dataMsg = std::move(dataMsg);
    msg.controlMsg = std::move(controlMsg);

    stdx::lock_guard<Latch> lock(_mutex);
    _messages.push(std::move(msg));
}

void InMemorySinkOperator::addControlMsgInner(StreamControlMsg controlMsg) {
    StreamMsgUnion msg;
    msg.controlMsg = std::move(controlMsg);

    stdx::lock_guard<Latch> lock(_mutex);
    _messages.push(std::move(msg));
}

std::queue<StreamMsgUnion> InMemorySinkOperator::getMessages() {
    stdx::lock_guard<Latch> lock(_mutex);
    auto messages = std::move(_messages);
    _messages = std::queue<StreamMsgUnion>();
    return messages;
}

void InMemorySinkOperator::doSinkOnDataMsg(int32_t inputIdx,
                                           StreamDataMsg dataMsg,
                                           boost::optional<StreamControlMsg> controlMsg) {
    addDataMsgInner(std::move(dataMsg), std::move(controlMsg));
}

void InMemorySinkOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    addControlMsgInner(std::move(controlMsg));
}

}  // namespace streams
