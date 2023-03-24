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
    std::queue<KafkaSourceDocument> getMessages();

private:
    void doAddMessage(KafkaSourceDocument msg) override;

    // Guards _docs.
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("InMemoryDeadLetterQueue::mutex");
    std::queue<KafkaSourceDocument> _messages;
};

}  // namespace streams
