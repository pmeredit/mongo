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

InMemorySourceOperator::~InMemorySourceOperator() {
    // Report 0 memory usage to SourceBufferManager.
    _context->sourceBufferManager->allocPages(
        _sourceBufferHandle.get(), 0 /* curSize */, 0 /* numPages */);
}

void InMemorySourceOperator::addDataMsg(StreamDataMsg dataMsg,
                                        boost::optional<StreamControlMsg> controlMsg) {
    addDataMsgInner(std::move(dataMsg), std::move(controlMsg));
}

void InMemorySourceOperator::addDataMsgInner(StreamDataMsg dataMsg,
                                             boost::optional<StreamControlMsg> controlMsg) {
    StreamMsgUnion msg;
    msg.dataMsg = std::move(dataMsg);
    msg.dataMsg->creationTimer = mongo::Timer{};
    msg.controlMsg = std::move(controlMsg);

    // Report current memory usage to SourceBufferManager and allocate one page of memory from it.
    bool allocSuccess = _context->sourceBufferManager->allocPages(
        _sourceBufferHandle.get(), _stats.memoryUsageBytes /* curSize */, 1 /* numPages */);
    uassert(ErrorCodes::InternalError,
            "Failed to allocate a page from SourceBufferManager",
            allocSuccess);
    incOperatorStats(OperatorStats{.memoryUsageBytes = msg.dataMsg->getByteSize(),
                                   .timeSpent = msg.dataMsg->creationTimer->elapsed()});

    {
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        _messages.push_back(std::move(msg));
    }

    // Report current memory usage to SourceBufferManager.
    _context->sourceBufferManager->allocPages(
        _sourceBufferHandle.get(), _stats.memoryUsageBytes /* curSize */, 0 /* numPages */);
}

void InMemorySourceOperator::addControlMsg(StreamControlMsg controlMsg) {
    addControlMsgInner(std::move(controlMsg));
}

void InMemorySourceOperator::addControlMsgInner(StreamControlMsg controlMsg) {
    StreamMsgUnion msg;
    msg.controlMsg = std::move(controlMsg);

    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _messages.push_back(std::move(msg));
}

std::vector<StreamMsgUnion> InMemorySourceOperator::getMessages(WithLock) {
    std::vector<StreamMsgUnion> msgs;
    std::swap(_messages, msgs);
    _stats.memoryUsageBytes = 0;
    return msgs;
}

}  // namespace streams
