/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/log_dead_letter_queue.h"
#include "mongo/logv2/log.h"
#include "streams/exec/constants.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

void LogDeadLetterQueue::doAddMessage(KafkaSourceDocument msg) {
    int64_t logAppendTimeMs = msg.logAppendTimeMs ? *msg.logAppendTimeMs : -1;
    if (msg.doc) {
        LOGV2_INFO(ErrorCode::kTemporaryUserErrorCode,
                   "DLQ",
                   "data"_attr = msg.doc->toString(),
                   "logAppendTime"_attr = logAppendTimeMs,
                   "offset"_attr = msg.offset);
    } else {
        LOGV2_INFO(ErrorCode::kTemporaryUserErrorCode,
                   "DLQ",
                   "data"_attr = msg.docBuf->get(),
                   "logAppendTime"_attr = logAppendTimeMs,
                   "offset"_attr = msg.offset);
    }
}

}  // namespace streams
