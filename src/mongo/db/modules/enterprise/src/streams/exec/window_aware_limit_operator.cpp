/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/window_aware_limit_operator.h"

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

WindowAwareLimitOperator::WindowAwareLimitOperator(Context* context, Options options)
    : WindowAwareOperator(context), _options(std::move(options)) {}

void WindowAwareLimitOperator::doProcessDocs(Window* window,
                                             std::vector<StreamDocument> streamDocs) {
    window->stats.numInputDocs += streamDocs.size();
    auto limitWindow = getLimitWindow(window);
    invariant(_options.limit >= limitWindow->numSent);
    size_t numDocsToSend =
        std::min<size_t>(_options.limit - limitWindow->numSent, streamDocs.size());
    limitWindow->numSent += numDocsToSend;
    invariant(_options.limit >= limitWindow->numSent);
    if (numDocsToSend < streamDocs.size()) {
        streamDocs.erase(streamDocs.begin() + numDocsToSend, streamDocs.end());
    }
    invariant(numDocsToSend == streamDocs.size());

    StreamDataMsg msg;
    msg.docs = std::move(streamDocs);
    // Apply the window's stream meta to the output.
    for (auto& doc : msg.docs) {
        doc.streamMeta = window->streamMetaTemplate;
    }
    if (!msg.docs.empty()) {
        sendDataMsg(/*outputIdx*/ 0, std::move(msg));
    }
}

std::unique_ptr<WindowAwareOperator::Window> WindowAwareLimitOperator::doMakeWindow(
    Window baseState) {
    return std::make_unique<LimitWindow>(std::move(baseState));
}

void WindowAwareLimitOperator::doSaveWindowState(CheckpointStorage::WriterHandle* writer,
                                                 Window* window) {
    auto state = getLimitWindow(window);
    WindowOperatorLimitRecord limitRecord;
    WindowOperatorCheckpointRecord record;
    limitRecord.setNumSent(state->numSent);
    record.setLimitRecord(std::move(limitRecord));
    _context->checkpointStorage->appendRecord(writer, Document{record.toBSON()});
}

void WindowAwareLimitOperator::doRestoreWindowState(Window* window, mongo::Document obj) {
    IDLParserContext parserContext("WindowAwareLimitOperatorCheckpointRestore");
    auto record = WindowOperatorCheckpointRecord::parse(parserContext, obj.toBson());
    auto limitRecord = record.getLimitRecord();
    CHECKPOINT_RECOVERY_ASSERT(8248200,
                               _operatorId,
                               "Limit record field missing from checkpoint restore record",
                               limitRecord);

    auto state = getLimitWindow(window);
    state->numSent = limitRecord->getNumSent();
}

WindowAwareLimitOperator::LimitWindow* WindowAwareLimitOperator::getLimitWindow(
    WindowAwareOperator::Window* window) {
    auto limitWindow = dynamic_cast<LimitWindow*>(window);
    invariant(limitWindow);
    return limitWindow;
}

}  // namespace streams
