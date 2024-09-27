/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

#include "mongo/logv2/log.h"

#include "streams/exec/in_memory_dead_letter_queue.h"

namespace streams {

using namespace mongo;

std::queue<mongo::BSONObj> InMemoryDeadLetterQueue::getMessages() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    auto messages = std::move(_messages);
    _messages = std::queue<mongo::BSONObj>();
    return messages;
}

int InMemoryDeadLetterQueue::doAddMessage(mongo::BSONObj msg) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    int objSize = msg.objsize();
    _messageBytes += objSize;
    _messages.push(std::move(msg));

    return objSize;
}

}  // namespace streams
