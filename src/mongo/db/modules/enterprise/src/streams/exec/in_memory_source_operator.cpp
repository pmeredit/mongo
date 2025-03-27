/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/in_memory_source_operator.h"

#include "mongo/bson/oid.h"
#include "mongo/platform/basic.h"
#include "streams/exec/context.h"
#include "streams/exec/message.h"
#include "streams/exec/util.h"

using namespace mongo;

namespace streams {

namespace {

StreamDataMsg generateFixedDataMsg(int docSize, int messageSize) {
    auto randomDoc = [&](int i) {
        static const char characters[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        auto seed = std::random_device{}();
        std::mt19937 gen;
        gen.seed(seed);
        std::string tmp;
        tmp.reserve(docSize);
        for (int i = 0; i < docSize; ++i) {
            tmp += characters[gen() % (sizeof(characters) - 1)];
        }
        return BSON("i" << i << "str" << tmp);
    };
    std::vector<StreamDocument> docs;
    int curSize = 0;
    int idx = 0;
    while (curSize < messageSize) {
        docs.push_back(StreamDocument(Document(randomDoc(idx++))));
        curSize += serializeJson(docs.back().doc.toBson(), JsonStringFormat::Relaxed).size();
    }
    StreamDataMsg msg;
    msg.docs = std::move(docs);
    return msg;
}

}  // namespace

InMemorySourceOperator::InMemorySourceOperator(Context* context, Options options)
    : GeneratedDataSourceOperator(context, /* numOutputs */ 1), _options(std::move(options)) {
    _oneMBMessage = generateFixedDataMsg(1.5 * 1024, 900 * 1024);
}

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
                                   .timeSpent = msg.dataMsg->creationTimer.elapsed()});

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
    auto useConstantMessage =
        _context->featureFlags->getFeatureFlagValue(FeatureFlags::kEnableInMemoryConstantMessage)
            .getBool();
    if (useConstantMessage && *useConstantMessage) {
        return {StreamMsgUnion{.dataMsg = _oneMBMessage}};
    }

    std::vector<StreamMsgUnion> msgs;
    std::swap(_messages, msgs);
    _stats.memoryUsageBytes = 0;
    return msgs;
}

}  // namespace streams
