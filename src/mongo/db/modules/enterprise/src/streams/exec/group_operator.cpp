/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/group_operator.h"

#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

GroupOperator::GroupOperator(Context* context, Options options)
    : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1),
      _options(std::move(options)),
      _processor(_options.documentSource->getGroupProcessor()) {
    _processor->setExecutionStarted();
}

void GroupOperator::doOnDataMsg(int32_t inputIdx,
                                StreamDataMsg dataMsg,
                                boost::optional<StreamControlMsg> controlMsg) {
    for (auto& streamDoc : dataMsg.docs) {
        Value id;
        try {
            id = _processor->computeId(streamDoc.doc);
            // TODO: Also catch any exception thrown while evaluating accumulator arguments.
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            _context->dlq->addMessage(toDeadLetterQueueMsg(streamDoc.streamMeta, std::move(error)));

            continue;  // Process next doc.
        }

        // Let any exceptions that occur here escape to WindowOperator.
        _processor->add(id, streamDoc.doc);
    }

    if (controlMsg) {
        onControlMsg(/*outputIdx*/ 0, std::move(*controlMsg));
    }
}

void GroupOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
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
        _processor->readyGroups();

        // We believe no exceptions related to data errors should occur at this point.
        // But if any unexpected exceptions occur, we let them escape and stop the pipeline for now.
        StreamDataMsg outputMsg = newStreamDataMsg();
        auto result = _processor->getNext();
        while (result) {
            curDataMsgByteSize += result->getApproximateSize();
            outputMsg.docs.emplace_back(std::move(*result));
            if (outputMsg.docs.size() == kDataMsgMaxDocSize ||
                curDataMsgByteSize >= kDataMsgMaxByteSize) {
                sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg));
                outputMsg = newStreamDataMsg();
            }
            result = _processor->getNext();
        }

        if (!outputMsg.docs.empty()) {
            sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg));
        }
    }

    sendControlMsg(inputIdx, std::move(controlMsg));
}

}  // namespace streams
