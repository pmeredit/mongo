/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#include "streams/exec/in_memory_source_sink_operator.h"

namespace streams {

using namespace mongo;

InMemorySourceSinkOperator::InMemorySourceSinkOperator(int32_t numInputs, int32_t numOutputs)
    : Operator(numInputs, numOutputs) {
    dassert(numInputs == 0 || numOutputs == 0);
    dassert(numInputs != 0 || numOutputs != 0);
    dassert(numOutputs != 0 || numOutputs == 1);  // At most only 1 output is allowed.
}

void InMemorySourceSinkOperator::addDataMsg(StreamDataMsg dataMsg,
                                            boost::optional<StreamControlMsg> controlMsg) {
    dassert(isSource());
    addDataMsgInner(std::move(dataMsg), std::move(controlMsg));
}

void InMemorySourceSinkOperator::addDataMsgInner(StreamDataMsg dataMsg,
                                                 boost::optional<StreamControlMsg> controlMsg) {
    dassert(isSource());

    StreamMsgUnion msg;
    msg.dataMsg = std::move(dataMsg);
    msg.controlMsg = std::move(controlMsg);
    _messages.push(std::move(msg));
}

void InMemorySourceSinkOperator::addControlMsg(StreamControlMsg controlMsg) {
    dassert(isSource());
    addControlMsgInner(std::move(controlMsg));
}

void InMemorySourceSinkOperator::addControlMsgInner(StreamControlMsg controlMsg) {
    dassert(isSource());

    StreamMsgUnion msg;
    msg.controlMsg = std::move(controlMsg);
    _messages.push(std::move(msg));
}

void InMemorySourceSinkOperator::runOnce() {
    dassert(isSource());

    while (!_messages.empty()) {
        StreamMsgUnion msg = std::move(_messages.front());
        _messages.pop();

        if (msg.dataMsg) {
            sendDataMsg(/*outputIdx*/ 0, std::move(msg.dataMsg.get()), std::move(msg.controlMsg));
        } else {
            sendControlMsg(/*outputIdx*/ 0, std::move(msg.controlMsg.get()));
        }
    }
}

void InMemorySourceSinkOperator::doOnDataMsg(int32_t inputIdx,
                                             StreamDataMsg dataMsg,
                                             boost::optional<StreamControlMsg> controlMsg) {
    dassert(isSink());
    addDataMsgInner(std::move(dataMsg), std::move(controlMsg));
}

void InMemorySourceSinkOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    dassert(isSink());
    addControlMsgInner(std::move(controlMsg));
}

}  // namespace streams
