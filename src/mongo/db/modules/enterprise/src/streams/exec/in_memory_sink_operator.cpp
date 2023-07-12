/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/in_memory_sink_operator.h"

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

InMemorySinkOperator::InMemorySinkOperator(Context* context, int32_t numInputs)
    : CollectOperator(context, numInputs) {
    dassert(numInputs != 0);
}

std::queue<StreamMsgUnion> InMemorySinkOperator::doGetMessages() {
    stdx::lock_guard<Latch> lock(_mutex);
    return CollectOperator::doGetMessages();
}

void InMemorySinkOperator::doSinkOnDataMsg(int32_t inputIdx,
                                           StreamDataMsg dataMsg,
                                           boost::optional<StreamControlMsg> controlMsg) {
    stdx::lock_guard<Latch> lock(_mutex);
    return CollectOperator::doSinkOnDataMsg(inputIdx, std::move(dataMsg), std::move(controlMsg));
}

void InMemorySinkOperator::doSinkOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    stdx::lock_guard<Latch> lock(_mutex);
    return CollectOperator::doSinkOnControlMsg(inputIdx, std::move(controlMsg));
}

}  // namespace streams
