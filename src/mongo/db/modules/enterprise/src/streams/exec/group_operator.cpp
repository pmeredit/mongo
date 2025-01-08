/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/group_operator.h"

#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/group_processor.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/util.h"
#include "streams/exec/window_aware_operator.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

GroupOperator::GroupOperator(Context* context, Options options)
    : WindowAwareOperator(context), _options(std::move(options)) {}

void GroupOperator::doProcessDocs(Window* window, std::vector<StreamDocument> streamDocs) {
    auto* groupWindow = getGroupWindow(window);
    auto& processor = groupWindow->processor;

    for (const auto& streamDoc : streamDocs) {
        Value groupKey;
        boost::optional<mongo::GroupProcessor::GroupsMap::iterator> groupIter;
        std::vector<boost::optional<Value>> accumulatorArgs;
        try {
            // Do as much document processing as we can here without modifying the internal state
            // of '_processor'. If any errors are encountered, we simply add the document to the
            // dead letter queue and move on.
            groupKey = processor->computeGroupKey(streamDoc.doc);
            groupIter = processor->findGroup(groupKey);
            processor->computeAccumulatorArgs(groupIter, streamDoc.doc, &accumulatorArgs);
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            auto numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(
                _context->streamMetaFieldName, streamDoc, getName(), std::move(error)));
            incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});

            continue;
        }

        if (!groupIter) {
            bool inserted{false};
            std::tie(groupIter, inserted) = processor->findOrCreateGroup(groupKey);
            invariant(groupIter);
            invariant(inserted);
        }

        window->minEventTimestampMs =
            std::min(window->minEventTimestampMs, streamDoc.minDocTimestampMs);
        window->maxEventTimestampMs =
            std::max(window->maxEventTimestampMs, streamDoc.maxDocTimestampMs);

        processor->accumulate(*groupIter, accumulatorArgs);

        groupWindow->stats.numInputDocs++;
        updateStats(window);
    }
}

std::unique_ptr<WindowAwareOperator::Window> GroupOperator::doMakeWindow(Window baseState) {
    auto documentSource = _options.documentSource->clone(_options.documentSource->getContext());
    auto groupDocumentSource = dynamic_cast<DocumentSourceGroup*>(documentSource.get());
    invariant(groupDocumentSource);
    invariant(groupDocumentSource->getGroupProcessor());
    auto memoryUsageHandle = _context->memoryAggregator->createUsageHandle();
    auto processor = std::make_unique<GroupProcessor>(groupDocumentSource->getGroupProcessor());
    processor->setExecutionStarted();
    return std::make_unique<GroupWindow>(std::move(baseState),
                                         std::move(groupDocumentSource),
                                         std::move(processor),
                                         std::move(memoryUsageHandle));
}

void GroupOperator::doCloseWindow(Window* window) {
    auto& processor = getGroupWindow(window)->processor;
    int64_t curDataMsgByteSize;

    auto newStreamDataMsg = [&]() {
        StreamDataMsg outputMsg;
        auto capacity = std::min(window->stats.numInputDocs, kDataMsgMaxDocSize);
        outputMsg.docs.reserve(capacity);
        curDataMsgByteSize = 0;
        return outputMsg;
    };

    processor->readyGroups();

    // We believe no exceptions related to data errors should occur at this point.
    // But if any unexpected exceptions occur, we let them escape and fail the pipeline.
    StreamDataMsg outputMsg = newStreamDataMsg();
    auto result = processor->getNext();
    while (result) {
        curDataMsgByteSize += result->getApproximateSize();

        auto doc = std::move(*result);
        if (_context->shouldProjectStreamMetaPriorToSinkStage()) {
            auto newStreamMeta = updateStreamMeta(doc.getField(*_context->streamMetaFieldName),
                                                  window->streamMetaTemplate);
            MutableDocument mutableDoc(std::move(doc));
            mutableDoc.setField(*_context->streamMetaFieldName, Value(std::move(newStreamMeta)));
            doc = mutableDoc.freeze();
        }
        StreamDocument streamDoc(std::move(doc));
        streamDoc.streamMeta = window->streamMetaTemplate;
        streamDoc.minDocTimestampMs = window->minEventTimestampMs;
        streamDoc.maxDocTimestampMs = window->maxEventTimestampMs;
        outputMsg.docs.emplace_back(std::move(streamDoc));
        if (outputMsg.docs.size() == kDataMsgMaxDocSize ||
            curDataMsgByteSize >= kDataMsgMaxByteSize) {
            incOperatorStats({.timeSpent = window->creationTimer.elapsed()});
            sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg));
            outputMsg = newStreamDataMsg();
        }
        result = processor->getNext();
    }

    if (!outputMsg.docs.empty()) {
        incOperatorStats({.timeSpent = window->creationTimer.elapsed()});
        sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg));
    }
}

void GroupOperator::doUpdateStats(Window* window) {
    auto group = getGroupWindow(window);
    auto& processor = group->processor;
    auto bytes = processor->getMemoryUsageBytes();
    group->memoryUsageHandle.set(bytes);
    window->stats.memoryUsageBytes = bytes;
}

GroupOperator::GroupWindow* GroupOperator::getGroupWindow(WindowAwareOperator::Window* window) {
    auto groupState = dynamic_cast<GroupWindow*>(window);
    invariant(groupState);
    return groupState;
}

void GroupOperator::doSaveWindowState(CheckpointStorage::WriterHandle* writer, Window* window) {
    auto& processor = getGroupWindow(window)->processor;
    processor->readyGroups();

    while (processor->hasNext()) {
        auto [key, accumulators] = processor->getNextGroup();
        MutableDocument groupRecord;
        groupRecord.addField(WindowAwareGroupRecord::kGroupKeyFieldName, std::move(key));
        groupRecord.addField(WindowAwareGroupRecord::kGroupAccumulatorsFieldName,
                             std::move(accumulators));

        MutableDocument checkpointRecord;
        checkpointRecord.addField(WindowOperatorCheckpointRecord::kGroupRecordFieldName,
                                  std::move(groupRecord).freezeToValue());
        _context->checkpointStorage->appendRecord(writer, checkpointRecord.freeze());
    }
}

void GroupOperator::doRestoreWindowState(Window* window, Document record) {
    auto& processor = getGroupWindow(window)->processor;
    // Temporarily enabling the merging mode since the group accumulators state was checkpointed
    // as partial with AccumulatorState::getValue(true)
    processor->setDoingMerge(true);
    auto groupRecord = record.getField(WindowOperatorCheckpointRecord::kGroupRecordFieldName);
    CHECKPOINT_RECOVERY_ASSERT(8249930,
                               _operatorId,
                               "Missing checkpoint record for the group operator.",
                               !groupRecord.missing());

    auto key = groupRecord.getDocument().getField(WindowAwareGroupRecord::kGroupKeyFieldName);
    auto accumulatorsRecord =
        groupRecord.getDocument().getField(WindowAwareGroupRecord::kGroupAccumulatorsFieldName);
    CHECKPOINT_RECOVERY_ASSERT(8249931,
                               _operatorId,
                               "Missing Key or Accumulator record from the recovered group record.",
                               !key.missing() && !accumulatorsRecord.missing() &&
                                   accumulatorsRecord.isArray());

    const auto& accumulators = accumulatorsRecord.getArray();

    getGroupWindow(window)->processor->addGroup(std::move(key), accumulators);
    // disable the merging mode after all the groups are added back as part of group operator
    // restore
    processor->setDoingMerge(false);
}

void GroupOperator::GroupWindow::doMerge(Window* other) {
    /*
    Retrieve the mergeable key/accumulator pairs from the rightWindow, feed them into the
    leftWindow’s processor with addGroup.
    */

    auto otherGroupState = dynamic_cast<GroupWindow*>(other);
    auto& otherProcessor = otherGroupState->processor;

    auto& thisProcessor = processor;

    otherProcessor->readyGroups();
    thisProcessor->setDoingMerge(true);
    while (otherProcessor->hasNext()) {
        auto [key, accumulators] = otherProcessor->getNextGroup();
        thisProcessor->addGroup(std::move(key), accumulators.getArray(), true);
    }

    thisProcessor->setDoingMerge(false);
}

}  // namespace streams
