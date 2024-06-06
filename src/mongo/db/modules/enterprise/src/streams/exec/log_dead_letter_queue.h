/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/platform/basic.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/message.h"

namespace streams {

struct Context;

/**
 * LogDeadLetterQueue prints a log message to stdout when documents are DLQ-ed.
 */
class LogDeadLetterQueue : public DeadLetterQueue {
public:
    LogDeadLetterQueue(Context* context) : DeadLetterQueue(context) {}

private:
    int doAddMessage(mongo::BSONObj msg) override;
};

}  // namespace streams
