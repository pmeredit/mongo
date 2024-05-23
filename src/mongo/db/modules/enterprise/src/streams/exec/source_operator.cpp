/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/source_operator.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/message.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

SourceOperator::SourceOperator(Context* context, int32_t numOutputs)
    : Operator(context, /*numInputs*/ 0, numOutputs) {}

int64_t SourceOperator::runOnce() {
    _operatorTimer.unpause();
    ScopeGuard guard([&] { _operatorTimer.pause(); });

    const auto numDocsConsumed = doRunOnce();
    return numDocsConsumed;
}

void SourceOperator::doIncOperatorStats(OperatorStats stats) {
    Operator::doIncOperatorStats(std::move(stats));
}

}  // namespace streams
