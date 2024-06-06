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
 * This is a test-only DeadLetterQueue implementation that does nothing.
 */
class NoOpDeadLetterQueue : public DeadLetterQueue {
public:
    NoOpDeadLetterQueue(Context* context) : DeadLetterQueue(context) {}

private:
    int doAddMessage(mongo::BSONObj msg) override {
        return 0;
    }
};

}  // namespace streams
