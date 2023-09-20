/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/log_util.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

mongo::BSONObj toBSON(Context* context) {
    if (!context) {
        LOGV2_WARNING(76898, "context is nullptr during log");
        return mongo::BSONObj();
    }
    return context->toBSON();
}

}  // namespace streams
