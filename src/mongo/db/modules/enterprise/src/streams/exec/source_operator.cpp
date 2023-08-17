/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/source_operator.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/util/metric_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

SourceOperator::SourceOperator(Context* context, int32_t numOutputs)
    : Operator(context, /*numInputs*/ 0, numOutputs) {
    MetricManager::LabelsVec labels;
    labels.push_back(std::make_pair(kTenantIdLabelKey, _context->tenantId));
    labels.push_back(std::make_pair(kProcessorIdLabelKey, _context->streamProcessorId));
    _numInputDocumentsCounter =
        _context->metricManager->registerCounter("num_input_documents", labels);
    _numInputBytesCounter = _context->metricManager->registerCounter("num_input_bytes", labels);
}

int64_t SourceOperator::runOnce() {
    return doRunOnce();
}

void SourceOperator::doIncOperatorStats(OperatorStats stats) {
    _numInputDocumentsCounter->increment(stats.numInputDocs);
    _numInputBytesCounter->increment(stats.numInputBytes);
    Operator::doIncOperatorStats(std::move(stats));
}

}  // namespace streams
