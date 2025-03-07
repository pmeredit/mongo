/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/s3_emit_operator.h"

namespace streams {

S3EmitOperator::S3EmitOperator(Context* context, S3EmitOperator::Options options)
    : QueuedSinkOperator(context, 1 /* numInputs */, 1 /* parallelism */),
      _options(std::move(options)) {}

OperatorStats S3EmitWriter::processDataMsg(StreamDataMsg dataMsg) {
    return OperatorStats{};
}

void S3EmitWriter::connect() {
    return;
}

std::unique_ptr<SinkWriter> S3EmitOperator::makeWriter(int id) {
    return std::make_unique<S3EmitWriter>(_context, this);
}

}  // namespace streams
