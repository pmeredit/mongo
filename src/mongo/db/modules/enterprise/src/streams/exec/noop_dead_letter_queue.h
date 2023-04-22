#pragma once

#include "mongo/platform/basic.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/message.h"

namespace streams {

/**
 * This is a test-only DeadLetterQueue implementation that does nothing.
 */
class NoOpDeadLetterQueue : public DeadLetterQueue {
public:
    NoOpDeadLetterQueue(mongo::NamespaceString ns) : DeadLetterQueue(std::move(ns)) {}

private:
    void doAddMessage(mongo::BSONObj msg) override {}
};

}  // namespace streams
