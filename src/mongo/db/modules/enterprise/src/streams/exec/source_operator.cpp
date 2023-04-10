/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/source_operator.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

int32_t SourceOperator::runOnce() {
    return doRunOnce();
}

}  // namespace streams
