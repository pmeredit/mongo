/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/window_aware_sort_operator.h"

#include "mongo/db/pipeline/document_source_sort.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/util.h"
#include "streams/exec/window_aware_operator.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;

namespace streams {

WindowAwareSortOperator::WindowAwareSortOperator(Context* context, Options options)
    : WindowAwareOperator(context), _options(std::move(options)) {}

void WindowAwareSortOperator::doProcessDocs(Window* window,
                                            std::vector<StreamDocument> streamDocs) {
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
            _context->dlq->addMessage(toDeadLetterQueueMsg(streamDoc.streamMeta, std::move(error)));
            incOperatorStats({.numDlqDocs = 1});
            return;
        }

        processor->add(sortKey, streamDoc.doc);
    }
    window->stats.numInputDocs += streamDocs.size();
}

void WindowAwareSortOperator::doCloseWindow(Window* window) {
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
        streamDoc.streamMeta = window->streamMetaTemplate;
        outputMsg.docs.emplace_back(std::move(streamDoc));
        if (outputMsg.docs.size() == kDataMsgMaxDocSize ||
            curDataMsgByteSize >= kDataMsgMaxByteSize) {
            sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg));
            outputMsg = newStreamDataMsg();
        }
    }

    sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), boost::none);
}

std::unique_ptr<WindowAwareOperator::Window> WindowAwareSortOperator::doMakeWindow(
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
    return std::make_unique<SortWindow>(std::move(baseWindow),
                                        std::move(documentSource),
                                        std::move(processor),
                                        std::move(sortKeyGenerator));
}

void WindowAwareSortOperator::doUpdateStats(Window* window) {
    window->stats.memoryUsageBytes = getSortWindow(window)->processor->stats().totalDataSizeBytes;
}

WindowAwareSortOperator::SortWindow* WindowAwareSortOperator::getSortWindow(
    WindowAwareOperator::Window* window) {
    auto sortState = dynamic_cast<SortWindow*>(window);
    invariant(sortState);
    return sortState;
}

void WindowAwareSortOperator::doSaveWindowState(CheckpointStorage::WriterHandle* writer,
                                                Window* window) {
    // Save the data in the sortExecutor using _context.checkpointStorage->appendRecord.
    // auto sortExecutor = getSortWindow(window)->processor.get();
    MONGO_UNIMPLEMENTED;
}

void WindowAwareSortOperator::doRestoreWindowState(Window* window, BSONObj record) {
    // Read the bson into the sort executor.
    // auto sortExecutor = getSortWindow(window)->processor.get();
    MONGO_UNIMPLEMENTED;
}

}  // namespace streams
