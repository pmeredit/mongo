/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/log_dead_letter_queue.h"
#include "mongo/logv2/log.h"
#include "streams/exec/constants.h"
#include "streams/exec/log_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

int LogDeadLetterQueue::doAddMessage(mongo::BSONObj msg) {
    LOGV2_INFO(75904, "DLQ", "msg"_attr = msg, "context"_attr = _context);
    return 0;
}

}  // namespace streams
