/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/sort_operator.h"

#include <limits>

#include "mongo/db/pipeline/document_source_sort.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

SortOperator::SortOperator(Context* context, Options options)
    : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1), _options(std::move(options)) {
    auto sortExecutor = _options.documentSource->getSortExecutor();
    _processor.emplace(
        SortExecutor<Document>(sortExecutor->sortPattern(),
                               sortExecutor->getLimit(),
                               /*maxMemoryUsageBytes*/ std::numeric_limits<uint64_t>::max(),
                               /*tempDir*/ "",
                               /*allowDiskUse*/ false,
                               /*moveSortedDataToIterator*/ true));
    _sortKeyGenerator =
        SortKeyGenerator(sortExecutor->sortPattern(), context->expCtx->getCollator());
}

void SortOperator::doOnDataMsg(int32_t inputIdx,
                               StreamDataMsg dataMsg,
                               boost::optional<StreamControlMsg> controlMsg) {
    for (auto& streamDoc : dataMsg.docs) {
        if (!_streamMetaTemplate) {
            _streamMetaTemplate = streamDoc.streamMeta;
        }

        Value sortKey;
        try {
            sortKey = _sortKeyGenerator->computeSortKeyFromDocument(streamDoc.doc);
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            auto numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(
                _context->streamMetaFieldName, streamDoc.streamMeta, std::move(error)));
            incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});

            continue;  // Process next doc.
        }

        // Let any exceptions that occur here escape to WindowOperator.
        _processor->add(sortKey, streamDoc.doc);
    }

    _memoryUsageHandle.set(_processor->stats().memoryUsageBytes);

    if (controlMsg) {
        onControlMsg(/*outputIdx*/ 0, std::move(*controlMsg));
    }
}

void SortOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    if (controlMsg.eofSignal) {
        // `processEof` is responsible for sending the EOF signal.
        controlMsg.eofSignal = false;
        processEof();
    }

    if (!controlMsg.empty()) {
        sendControlMsg(/*outputIdx*/ 0, std::move(controlMsg));
    }
}

void SortOperator::processEof() {
    if (_reachedEof) {
        sendControlMsg(/*outputIdx*/ 0, StreamControlMsg{.eofSignal = true});
        return;
    }

    int32_t curDataMsgByteSize{0};
    StreamDataMsg outputMsg;
    outputMsg.docs.reserve(kDataMsgMaxDocSize);

    if (!_receivedEof) {
        _processor->loadingDone();
        _receivedEof = true;
    }

    // We believe no exceptions related to data errors should occur at this point.
    // But if any unexpected exceptions occur, we let them escape and stop the pipeline for now.
    auto streamMeta = getStreamMeta();
    while (_processor->hasNext() && outputMsg.docs.size() < kDataMsgMaxDocSize &&
           curDataMsgByteSize < kDataMsgMaxByteSize) {
        auto result = std::move(_processor->getNext().second);
        curDataMsgByteSize += result.getApproximateSize();

        StreamDocument streamDoc(std::move(result));
        streamDoc.streamMeta = streamMeta;
        outputMsg.docs.emplace_back(std::move(streamDoc));
    }

    boost::optional<StreamControlMsg> controlMsg;
    _reachedEof = !_processor->hasNext();
    if (_reachedEof) {
        controlMsg = StreamControlMsg{.eofSignal = true};
    }

    dassert(!outputMsg.docs.empty());
    sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), std::move(controlMsg));
    _memoryUsageHandle.set(_processor->stats().memoryUsageBytes);
}

StreamMeta SortOperator::getStreamMeta() {
    StreamMeta streamMeta;
    if (_streamMetaTemplate) {
        streamMeta.setSourceType(_streamMetaTemplate->getSourceType());
        streamMeta.setWindowStartTimestamp(_streamMetaTemplate->getWindowStartTimestamp());
        streamMeta.setWindowEndTimestamp(_streamMetaTemplate->getWindowEndTimestamp());
    }
    return streamMeta;
}

OperatorStats SortOperator::doGetStats() {
    _stats.memoryUsageBytes = _memoryUsageHandle.getCurrentMemoryUsageBytes();
    return _stats;
}

}  // namespace streams
