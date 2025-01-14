/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/sort_operator.h"

#include "mongo/db/pipeline/document_source_sort.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/log_util.h"
#include "streams/exec/util.h"
#include "streams/exec/window_aware_operator.h"
#include <algorithm>
#include <memory>

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;

namespace streams {

namespace {
static const int kSortOperatorMaxRecordSizeLimit = 10 * 1024 * 1024;
}

SortOperator::SortOperator(Context* context, Options options)
    : WindowAwareOperator(context), _options(std::move(options)) {}

void SortOperator::doProcessDocs(Window* window, std::vector<StreamDocument> streamDocs) {
    auto sortState = getSortWindow(window);
    auto& processor = sortState->processor;
    auto& sortKeyGenerator = sortState->sortKeyGenerator;

    for (const auto& streamDoc : streamDocs) {
        Value sortKey;
        try {
            sortKey = sortKeyGenerator->computeSortKeyFromDocument(streamDoc.doc);
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            auto numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(
                _context->streamMetaFieldName, streamDoc, getName(), std::move(error)));
            incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});
            continue;
        }

        window->minEventTimestampMs =
            std::min(window->minEventTimestampMs, streamDoc.minDocTimestampMs);
        window->maxEventTimestampMs =
            std::max(window->maxEventTimestampMs, streamDoc.maxDocTimestampMs);

        processor->add(sortKey, streamDoc.doc);

        window->stats.numInputDocs++;
        updateStats(window);
    }
}

void SortOperator::doCloseWindow(Window* window) {
    auto& processor = getSortWindow(window)->processor;

    processor->loadingDone();

    int32_t curDataMsgByteSize{0};
    auto newStreamDataMsg = [&]() {
        StreamDataMsg outputMsg;
        auto capacity = std::min(window->stats.numInputDocs, kDataMsgMaxDocSize);
        outputMsg.docs.reserve(capacity);
        curDataMsgByteSize = 0;
        return outputMsg;
    };

    // We believe no exceptions related to data errors should occur at this point.
    // But if any unexpected exceptions occur, we let them escape and stop the pipeline for now.
    StreamDataMsg outputMsg = newStreamDataMsg();
    while (processor->hasNext()) {
        auto result = std::move(processor->getNext().second);
        curDataMsgByteSize += result.getApproximateSize();
        StreamDocument streamDoc(std::move(result));
        // Don't update the internal DocumentMetadataFields.
        // Before a document enters a $sort stage, the $source stage might have set
        // some source metadata (topic, offset, etc.). We don't want to overwrite that
        // metadata in this stage's output.
        // TODO(SERVER-99097): Read DocumentMetadataFields and put it
        // back into the StreamDocument::streamMeta.
        streamDoc.streamMeta = window->streamMetaTemplate;
        streamDoc.windowId = window->windowID;
        streamDoc.minDocTimestampMs = window->minEventTimestampMs;
        streamDoc.maxDocTimestampMs = window->maxEventTimestampMs;
        outputMsg.docs.emplace_back(std::move(streamDoc));
        if (outputMsg.docs.size() == kDataMsgMaxDocSize ||
            curDataMsgByteSize >= kDataMsgMaxByteSize) {
            incOperatorStats({.timeSpent = window->creationTimer.elapsed()});
            sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg));
            outputMsg = newStreamDataMsg();
        }
    }
    if (!outputMsg.docs.empty()) {
        incOperatorStats({.timeSpent = window->creationTimer.elapsed()});
        sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), boost::none);
    }
}

std::unique_ptr<WindowAwareOperator::Window> SortOperator::doMakeWindow(
    WindowAwareOperator::Window baseWindow) {
    auto documentSource = _options.documentSource->clone(_options.documentSource->getContext());
    auto documentSourceSort = dynamic_cast<DocumentSourceSort*>(documentSource.get());
    invariant(documentSourceSort);
    auto sortExecutor = documentSourceSort->getSortExecutor();
    auto processor = std::make_unique<SortExecutor<Document>>(
        sortExecutor->sortPattern(),
        sortExecutor->getLimit(),
        /*maxMemoryUsageBytes*/ std::numeric_limits<uint64_t>::max(),
        /*tempDir*/ "",
        /*allowDiskUse*/ false,
        /*moveSortedDataToIterator*/ true);
    boost::optional<SortKeyGenerator> sortKeyGenerator(
        SortKeyGenerator{sortExecutor->sortPattern(), _context->expCtx->getCollator()});
    auto memoryUsageHandle = _context->memoryAggregator->createUsageHandle();
    return std::make_unique<SortWindow>(std::move(baseWindow),
                                        std::move(documentSource),
                                        std::move(processor),
                                        std::move(sortKeyGenerator),
                                        std::move(memoryUsageHandle));
}

void SortOperator::doUpdateStats(Window* window) {
    auto sortState = getSortWindow(window);
    auto processor = sortState->processor.get();
    auto bytes = processor->stats().memoryUsageBytes;
    sortState->memoryUsageHandle.set(bytes);
    window->stats.memoryUsageBytes = bytes;
}

SortOperator::SortWindow* SortOperator::getSortWindow(WindowAwareOperator::Window* window) {
    auto sortState = dynamic_cast<SortWindow*>(window);
    invariant(sortState);
    return sortState;
}

void SortOperator::doSaveWindowState(CheckpointStorage::WriterHandle* writer, Window* window) {
    auto processor = getSortWindow(window)->processor.get();
    processor->pauseLoading();
    ON_BLOCK_EXIT([&] { processor->resumeLoading(); });
    while (processor->hasNext()) {
        auto [key, value] = processor->getNext();
        MutableDocument record;
        record[WindowOperatorCheckpointRecord::kSortRecordFieldName] = Value{std::move(value)};
        _context->checkpointStorage->appendRecord(writer, record.freeze());
    }
}

void SortOperator::doRestoreWindowState(Window* window, Document record) {
    auto sortState = getSortWindow(window);
    auto processor = sortState->processor.get();
    auto& sortKeyGenerator = sortState->sortKeyGenerator;
    auto sortRecord = record.getField(WindowOperatorCheckpointRecord::kSortRecordFieldName);
    CHECKPOINT_RECOVERY_ASSERT(
        8289701,
        _operatorId,
        fmt::format("{kSortRecordFieldName} field missing from checkpoint restore record",
                    WindowOperatorCheckpointRecord::kSortRecordFieldName),
        !sortRecord.missing() && sortRecord.getType() == BSONType::Object);
    mongo::Document doc(sortRecord.getDocument());
    Value sortKey = sortKeyGenerator->computeSortKeyFromDocument(doc);
    processor->add(sortKey, std::move(doc));
}

void SortOperator::SortWindow::doMerge(Window* other) {
    auto otherSortState = dynamic_cast<SortWindow*>(other);
    auto& otherProcessor = otherSortState->processor;
    auto& thisProcessor = processor;
    otherProcessor->loadingDone();

    while (otherProcessor->hasNext()) {
        auto result = otherProcessor->getNext();
        thisProcessor->add(std::move(result.first), std::move(result.second));
        // DLQ should be impossible here
    }
}

}  // namespace streams
