/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/collect_operator.h"

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/context.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

CollectOperator::CollectOperator(Context* context, int32_t numInputs)
    : SinkOperator(context, numInputs) {
    dassert(numInputs != 0);
}

std::deque<StreamMsgUnion> CollectOperator::doGetMessages() {
    std::deque<StreamMsgUnion> messages;
    std::swap(messages, _messages);
    _memoryUsageHandle.set(0);
    return messages;
}

void CollectOperator::doSinkOnDataMsg(int32_t inputIdx,
                                      StreamDataMsg dataMsg,
                                      boost::optional<StreamControlMsg> controlMsg) {
    StreamMsgUnion msg;
    msg.dataMsg = std::move(dataMsg);
    msg.controlMsg = std::move(controlMsg);

    if (msg.dataMsg) {
        _memoryUsageHandle.add(msg.dataMsg->getByteSize());
    }
    sendOutputToSamplers(msg.dataMsg.get());
    _messages.push_back(std::move(msg));
}

void CollectOperator::doSinkOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    StreamMsgUnion msg;
    msg.controlMsg = std::move(controlMsg);
    _messages.push_back(std::move(msg));
}

}  // namespace streams
