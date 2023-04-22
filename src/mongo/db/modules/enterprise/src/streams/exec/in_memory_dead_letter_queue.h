#pragma once

#include <queue>
#include <string>

#include "mongo/platform/basic.h"
#include "mongo/platform/mutex.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/message.h"

namespace streams {

/**
 * This is a test-only DeadLetterQueue implementation that holds all the messages in memory.
 * This class is thread-safe.
 */
class InMemoryDeadLetterQueue : public DeadLetterQueue {
public:
    InMemoryDeadLetterQueue(mongo::NamespaceString ns) : DeadLetterQueue(std::move(ns)) {}

    std::queue<mongo::BSONObj> getMessages();

private:
    void doAddMessage(mongo::BSONObj msg) override;

    // Guards _docs.
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("InMemoryDeadLetterQueue::mutex");
    std::queue<mongo::BSONObj> _messages;
};

}  // namespace streams
