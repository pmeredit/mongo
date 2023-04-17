#pragma once

#include "mongo/platform/basic.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/message.h"

namespace streams {

/**
 * LogDeadLetterQueue prints a log message to stdout when documents are DLQ-ed.
 */
class LogDeadLetterQueue : public DeadLetterQueue {
private:
    void doAddMessage(KafkaSourceDocument msg) override;
};

}  // namespace streams
