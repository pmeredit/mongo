/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <algorithm>

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/output_sampler.h"
#include "streams/exec/sink_operator.h"
#include "streams/util/metric_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

SinkOperator::SinkOperator(Context* context, int32_t numInputs)
    : Operator(context, numInputs, /*numOutputs*/ 0) {
    MetricManager::LabelsVec labels;
    labels.push_back(std::make_pair(kTenantIdLabelKey, _context->tenantId));
    labels.push_back(std::make_pair(kProcessorIdLabelKey, _context->streamProcessorId));
    _numOutputDocumentsCounter =
        _context->metricManager->registerCounter("num_output_documents", labels);
    _numOutputBytesCounter = _context->metricManager->registerCounter("num_output_bytes", labels);
}

void SinkOperator::addOutputSampler(boost::intrusive_ptr<OutputSampler> sampler) {
    dassert(sampler);
    _outputSamplers.push_back(std::move(sampler));
}

void SinkOperator::sendOutputToSamplers(const StreamDataMsg& dataMsg) {
    if (_outputSamplers.empty()) {
        return;
    }

    for (auto& sampler : _outputSamplers) {
        sampler->addDataMsg(dataMsg);
    }

    // Prune the samplers that are done sampling.
    _outputSamplers.erase(std::remove_if(_outputSamplers.begin(),
                                         _outputSamplers.end(),
                                         [](const auto& sampler) {
                                             return sampler->doneSampling() ||
                                                 sampler->isCancelled();
                                         }),
                          _outputSamplers.end());
}

void SinkOperator::doOnDataMsg(int32_t inputIdx,
                               StreamDataMsg dataMsg,
                               boost::optional<StreamControlMsg> controlMsg) {
    // Add _stream_meta field to the documents.
    // TODO(SERVER-76802): We want to add _stream_meta to the documents much earlier instead
    // of doing it in the SinkOperator.
    for (auto& doc : dataMsg.docs) {
        auto streamMeta = doc.streamMeta.toBSON();
        if (streamMeta.isEmpty()) {
            continue;
        }
        MutableDocument mutableDoc{std::move(doc.doc)};
        mutableDoc.setField(kStreamsMetaField, Value(std::move(streamMeta)));
        doc.doc = mutableDoc.freeze();
    }

    sendOutputToSamplers(dataMsg);
    doSinkOnDataMsg(inputIdx, std::move(dataMsg), std::move(controlMsg));
}

void SinkOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    if (controlMsg.checkpointMsg) {
        // Note: right now we can always commit a checkpoint once the (one and only)
        // Sink receives it. This needs improvement when we support multiple sinks.
        flush();
        _context->checkpointStorage->commit(controlMsg.checkpointMsg->id);
    }

    doSinkOnControlMsg(inputIdx, std::move(controlMsg));
}

void SinkOperator::doIncOperatorStats(OperatorStats stats) {
    _numOutputDocumentsCounter->increment(stats.numInputDocs);
    _numOutputBytesCounter->increment(stats.numInputBytes);
    Operator::doIncOperatorStats(std::move(stats));
}

void SinkOperator::flush() {
    doFlush();
}

}  // namespace streams
