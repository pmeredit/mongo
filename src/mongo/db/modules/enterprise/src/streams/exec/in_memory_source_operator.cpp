/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/in_memory_source_operator.h"

#include "mongo/platform/basic.h"
#include "streams/exec/context.h"

namespace streams {

using namespace mongo;

InMemorySourceOperator::InMemorySourceOperator(Context* context, Options options)
    : GeneratedDataSourceOperator(context, /* numOutputs */ 1), _options(std::move(options)) {}

void InMemorySourceOperator::addDataMsg(StreamDataMsg dataMsg,
                                        boost::optional<StreamControlMsg> controlMsg) {
    addDataMsgInner(std::move(dataMsg), std::move(controlMsg));
}

void InMemorySourceOperator::addDataMsgInner(StreamDataMsg dataMsg,
                                             boost::optional<StreamControlMsg> controlMsg) {
    StreamMsgUnion msg;
    msg.dataMsg = std::move(dataMsg);
    msg.controlMsg = std::move(controlMsg);

    stdx::unique_lock<Latch> lock(_mutex);
    _messages.push_back(std::move(msg));
}

void InMemorySourceOperator::addControlMsg(StreamControlMsg controlMsg) {
    addControlMsgInner(std::move(controlMsg));
}

void InMemorySourceOperator::addControlMsgInner(StreamControlMsg controlMsg) {
    StreamMsgUnion msg;
    msg.controlMsg = std::move(controlMsg);

    stdx::lock_guard<Latch> lock(_mutex);
    _messages.push_back(std::move(msg));
}

std::vector<StreamMsgUnion> InMemorySourceOperator::getMessages(WithLock) {
    std::vector<StreamMsgUnion> msgs;
    std::swap(_messages, msgs);
    return msgs;
}

}  // namespace streams
