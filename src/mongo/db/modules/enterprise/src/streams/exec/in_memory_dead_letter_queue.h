/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <queue>
#include <string>

#include "mongo/platform/basic.h"
#include "mongo/platform/mutex.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/message.h"

namespace streams {

struct Context;

/**
 * This is a test-only DeadLetterQueue implementation that holds all the messages in memory.
 * This class is thread-safe.
 */
class InMemoryDeadLetterQueue : public DeadLetterQueue {
public:
    InMemoryDeadLetterQueue(Context* context) : DeadLetterQueue(context) {}

    // This moves the current _messages out
    std::queue<mongo::BSONObj> getMessages();

    size_t numMessages() const {
        return _messages.size();
    }

    size_t numMessageBytes() const {
        return _messageBytes;
    }

private:
    int doAddMessage(mongo::BSONObj msg) override;

    // Guards _docs.
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("InMemoryDeadLetterQueue::mutex");
    std::queue<mongo::BSONObj> _messages;
    int64_t _messageBytes{0};
};

}  // namespace streams
