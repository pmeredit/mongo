/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/context.h"

namespace streams {

mongo::BSONObj Context::toBSON() const {
    return BSON("streamProcessorName" << streamName << "streamProcessorId" << streamProcessorId
                                      << "tenantId" << tenantId);
}

Context::~Context() {
    // CheckpointStorage holds a MemoryHandle obtained from Context::memoryAggregator
    // So it needs to be destructed first. Instead of depending only on the
    // order of the member variables in Context which can be brittle, we
    // force it to be destructed first here.
    checkpointStorage.reset();
}

}  // namespace streams
