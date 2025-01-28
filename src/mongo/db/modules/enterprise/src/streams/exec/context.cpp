/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/context.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

mongo::BSONObj Context::toBSON() const {
    return BSON("streamProcessorName" << streamName << "streamProcessorId" << streamProcessorId
                                      << "tenantId" << tenantId << "projectId" << projectId);
}

Context::~Context() {
    // CheckpointStorage holds a MemoryHandle obtained from Context::memoryAggregator
    // So it needs to be destructed first. Instead of depending only on the
    // order of the member variables in Context which can be brittle, we
    // force it to be destructed first here.
    checkpointStorage.reset();
}

mongo::BSONObj toBSON(Context* context) {
    if (!context) {
        LOGV2_WARNING(76898, "context is nullptr during log");
        return mongo::BSONObj();
    }
    return context->toBSON();
}

}  // namespace streams
