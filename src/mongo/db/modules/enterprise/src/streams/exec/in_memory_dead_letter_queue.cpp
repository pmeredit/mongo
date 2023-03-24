/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "mongo/logv2/log.h"

#include "streams/exec/in_memory_dead_letter_queue.h"

namespace streams {

using namespace mongo;

std::queue<KafkaSourceDocument> InMemoryDeadLetterQueue::getMessages() {
    stdx::lock_guard<Latch> lock(_mutex);
    auto messages = std::move(_messages);
    _messages = std::queue<KafkaSourceDocument>();
    return messages;
}

void InMemoryDeadLetterQueue::doAddMessage(KafkaSourceDocument msg) {
    stdx::lock_guard<Latch> lock(_mutex);
    _messages.push(std::move(msg));
}

}  // namespace streams
