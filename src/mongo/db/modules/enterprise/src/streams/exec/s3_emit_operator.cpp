/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/s3_emit_operator.h"

namespace streams {

S3EmitOperator::S3EmitOperator(Context* context, S3EmitOperator::Options options)
    : QueuedSinkOperator(context, 1 /* numInputs */), _options(std::move(options)) {}

OperatorStats S3EmitOperator::processDataMsg(StreamDataMsg dataMsg) {
    return OperatorStats{};
}

void S3EmitOperator::validateConnection() {
    return;
}

}  // namespace streams
