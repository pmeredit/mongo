/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/group_operator.h"

#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/group_processor.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

GroupOperator::GroupOperator(Context* context, Options options)
    : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1),
      _options(std::move(options)),
      _processor(_options.documentSource->getGroupProcessor()) {
    _processor.setExecutionStarted();
}

void GroupOperator::doOnDataMsg(int32_t inputIdx,
                                StreamDataMsg dataMsg,
                                boost::optional<StreamControlMsg> controlMsg) {
    std::vector<boost::optional<Value>> accumulatorArgs;
    for (auto& streamDoc : dataMsg.docs) {
        if (!_streamMetaTemplate) {
            _streamMetaTemplate = streamDoc.streamMeta;
        }

        Value groupKey;
        boost::optional<mongo::GroupProcessor::GroupsMap::iterator> groupIter;
        try {
            // Do as much document processing as we can here without modifying the internal state
            // of '_processor'. If any errors are encountered, we simply add the document to the
            // dead letter queue and move on.
            groupKey = _processor.computeGroupKey(streamDoc.doc);
            groupIter = _processor.findGroup(groupKey);
            _processor.computeAccumulatorArgs(groupIter, streamDoc.doc, &accumulatorArgs);
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            auto numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(
                _context->streamMetaFieldName, streamDoc.streamMeta, std::move(error)));
            incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});

            continue;  // Process next doc.
        }

        if (!groupIter) {
            bool inserted{false};
            std::tie(groupIter, inserted) = _processor.findOrCreateGroup(groupKey);
            invariant(groupIter);
            invariant(inserted);
        }
        // Let any exceptions that occur here escape to WindowOperator.
        _processor.accumulate(*groupIter, accumulatorArgs);
    }

    _memoryUsageHandle.set(_processor.getMemoryUsageBytes());

    if (controlMsg) {
        onControlMsg(/*outputIdx*/ 0, std::move(*controlMsg));
    }
}

void GroupOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    if (controlMsg.eofSignal) {
        // `processEof` is responsible for sending the EOF signal.
        controlMsg.eofSignal = false;
        processEof();
    }

    if (!controlMsg.empty()) {
        sendControlMsg(/*outputIdx*/ 0, std::move(controlMsg));
    }
}

void GroupOperator::processEof() {
    if (_reachedEof) {
        sendControlMsg(/*outputIdx*/ 0, StreamControlMsg{.eofSignal = true});
        return;
    }

    int32_t curDataMsgByteSize{0};
    StreamDataMsg outputMsg;
    outputMsg.docs.reserve(kDataMsgMaxDocSize);

    if (!_receivedEof) {
        _processor.readyGroups();
        _receivedEof = true;
    }

    // We believe no exceptions related to data errors should occur at this point.
    // But if any unexpected exceptions occur, we let them escape and stop the pipeline for now.
    auto streamMeta = getStreamMeta();
    while (_processor.hasNext() && outputMsg.docs.size() < kDataMsgMaxDocSize &&
           curDataMsgByteSize < kDataMsgMaxByteSize) {
        auto result = _processor.getNext();
        curDataMsgByteSize += result->getApproximateSize();

        StreamDocument streamDoc(std::move(*result));
        streamDoc.streamMeta = streamMeta;
        outputMsg.docs.emplace_back(std::move(streamDoc));
    }

    boost::optional<StreamControlMsg> controlMsg;
    _reachedEof = !_processor.hasNext();
    if (_reachedEof) {
        controlMsg = StreamControlMsg{.eofSignal = true};
    }

    dassert(!outputMsg.docs.empty());
    sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), std::move(controlMsg));
}

StreamMeta GroupOperator::getStreamMeta() {
    StreamMeta streamMeta;
    if (_streamMetaTemplate) {
        streamMeta.setSourceType(_streamMetaTemplate->getSourceType());
        streamMeta.setWindowStartTimestamp(_streamMetaTemplate->getWindowStartTimestamp());
        streamMeta.setWindowEndTimestamp(_streamMetaTemplate->getWindowEndTimestamp());
    }
    return streamMeta;
}

OperatorStats GroupOperator::doGetStats() {
    _stats.memoryUsageBytes = _memoryUsageHandle.getCurrentMemoryUsageBytes();
    return _stats;
}

}  // namespace streams
