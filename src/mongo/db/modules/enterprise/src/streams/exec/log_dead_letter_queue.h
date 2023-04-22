#pragma once

#include "mongo/platform/basic.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/message.h"

namespace streams {

/**
 * LogDeadLetterQueue prints a log message to stdout when documents are DLQ-ed.
 */
class LogDeadLetterQueue : public DeadLetterQueue {
public:
    LogDeadLetterQueue(mongo::NamespaceString ns) : DeadLetterQueue(std::move(ns)) {}

private:
    void doAddMessage(mongo::BSONObj msg) override;
};

}  // namespace streams
