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
                               /*allowDiskUse*/ false));
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
            _context->dlq->addMessage(toDeadLetterQueueMsg(streamDoc.streamMeta, std::move(error)));
            incOperatorStats({.numDlqDocs = 1});

            continue;  // Process next doc.
        }

        // Let any exceptions that occur here escape to WindowOperator.
        _processor->add(sortKey, streamDoc.doc);
    }

    if (controlMsg) {
        onControlMsg(/*outputIdx*/ 0, std::move(*controlMsg));
    }
}

void SortOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    int32_t numInputDocs = _stats.numInputDocs;
    int32_t curDataMsgByteSize{0};
    auto newStreamDataMsg = [&]() {
        StreamDataMsg outputMsg;
        auto capacity = std::min(kDataMsgMaxDocSize, numInputDocs);
        outputMsg.docs.reserve(capacity);
        numInputDocs -= capacity;
        curDataMsgByteSize = 0;
        return outputMsg;
    };

    if (controlMsg.eofSignal) {
        _processor->loadingDone();

        // We believe no exceptions related to data errors should occur at this point.
        // But if any unexpected exceptions occur, we let them escape and stop the pipeline for now.
        auto streamMeta = getStreamMeta();
        StreamDataMsg outputMsg = newStreamDataMsg();
        while (_processor->hasNext()) {
            auto result = std::move(_processor->getNext().second);
            curDataMsgByteSize += result.getApproximateSize();

            StreamDocument streamDoc(std::move(result));
            streamDoc.streamMeta = streamMeta;
            outputMsg.docs.emplace_back(std::move(streamDoc));
            if (outputMsg.docs.size() == kDataMsgMaxDocSize ||
                curDataMsgByteSize >= kDataMsgMaxByteSize) {
                sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg));
                outputMsg = newStreamDataMsg();
            }
        }

        if (!outputMsg.docs.empty()) {
            sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg));
        }
    }

    sendControlMsg(inputIdx, std::move(controlMsg));
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
    _stats.memoryUsageBytes = _processor->stats().totalDataSizeBytes;
    return _stats;
}

}  // namespace streams
