/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/log_dead_letter_queue.h"
#include "mongo/logv2/log.h"
#include "streams/exec/constants.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

void LogDeadLetterQueue::doAddMessage(mongo::BSONObj msg) {
    LOGV2_INFO(75904, "DLQ", "msg"_attr = msg);
}

}  // namespace streams
