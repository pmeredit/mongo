#pragma once

#include "mongo/platform/basic.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/message.h"

namespace streams {

/**
 * This is a test-only DeadLetterQueue implementation that does nothing.
 */
class NoOpDeadLetterQueue : public DeadLetterQueue {
private:
    void doAddMessage(KafkaSourceDocument msg) override {}
};

}  // namespace streams
