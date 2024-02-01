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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

SinkOperator::SinkOperator(Context* context, int32_t numInputs)
    : Operator(context, numInputs, /*numOutputs*/ 0) {}

void SinkOperator::addOutputSampler(boost::intrusive_ptr<OutputSampler> sampler) {
    stdx::lock_guard<Latch> lock(_mutex);
    dassert(sampler);
    _outputSamplers.push_back(std::move(sampler));
}

void SinkOperator::sendOutputToSamplers(const StreamDataMsg& dataMsg) {
    stdx::lock_guard<Latch> lock(_mutex);
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
    auto sinkErr = getConnectionStatus();
    uassert(sinkErr.errorCode,
            fmt::format("sink error: {}", sinkErr.errorReason),
            sinkErr.isConnected());

    if (_context->shouldAddStreamMetaInSinkStage()) {
        for (auto& doc : dataMsg.docs) {
            auto streamMeta = doc.streamMeta.toBSON();
            if (streamMeta.isEmpty()) {
                continue;
            }
            MutableDocument mutableDoc{std::move(doc.doc)};
            mutableDoc.setField(*_context->streamMetaFieldName, Value(std::move(streamMeta)));
            doc.doc = mutableDoc.freeze();
        }
    }


    doSinkOnDataMsg(inputIdx, std::move(dataMsg), std::move(controlMsg));
}

void SinkOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    if (controlMsg.checkpointMsg) {
        // Note: right now we can always commit a checkpoint once the (one and only)
        // Sink receives it. This needs improvement when we support multiple sinks.
        _context->dlq->flush();
        flush();
        if (_context->oldCheckpointStorage) {
            _context->oldCheckpointStorage->addStats(
                controlMsg.checkpointMsg->id, _operatorId, getStats());
            _context->oldCheckpointStorage->commit(controlMsg.checkpointMsg->id);
        } else {
            _context->checkpointStorage->addStats(
                controlMsg.checkpointMsg->id, _operatorId, getStats());
            _context->checkpointStorage->commitCheckpoint(controlMsg.checkpointMsg->id);
        }
    }

    doSinkOnControlMsg(inputIdx, std::move(controlMsg));
}

void SinkOperator::doIncOperatorStats(OperatorStats stats) {
    Operator::doIncOperatorStats(std::move(stats));
}

void SinkOperator::flush() {
    doFlush();
}

bool SinkOperator::samplersExist() const {
    stdx::lock_guard<Latch> lock(_mutex);
    return !_outputSamplers.empty();
}

}  // namespace streams
