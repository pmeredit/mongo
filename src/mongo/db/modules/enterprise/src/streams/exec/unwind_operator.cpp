/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/unwind_operator.h"

#include "mongo/db/pipeline/document_source_unwind.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/util.h"

namespace streams {

using namespace mongo;

UnwindOperator::UnwindOperator(Context* context, Options options)
    : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1),
      _options(std::move(options)),
      _processor(_options.documentSource->getUnwindProcessor()) {}

void UnwindOperator::doOnDataMsg(int32_t inputIdx,
                                 StreamDataMsg dataMsg,
                                 boost::optional<StreamControlMsg> controlMsg) {
    int32_t curDataMsgByteSize{0};
    auto newStreamDataMsg = [&]() {
        StreamDataMsg outputMsg;
        outputMsg.docs.reserve(dataMsg.docs.size());
        curDataMsgByteSize = 0;
        return outputMsg;
    };

    StreamDataMsg outputMsg = newStreamDataMsg();
    size_t nextInputDocIdx{0};
    bool done{false};
    while (!done) {
        boost::optional<Document> resultDoc;
        try {
            resultDoc = _processor->getNext();
            while (!resultDoc) {
                if (nextInputDocIdx == dataMsg.docs.size()) {
                    done = true;
                    break;
                }
                const auto& streamDoc = dataMsg.docs[nextInputDocIdx++];
                _processor->process(streamDoc.doc);
                resultDoc = _processor->getNext();
            }
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            invariant(nextInputDocIdx > 0);
            const auto& streamDoc = dataMsg.docs[nextInputDocIdx - 1];
            auto numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(
                _context->streamMetaFieldName, streamDoc, getName(), std::move(error)));
            incOperatorStats({.numDlqDocs = 1, .numDlqBytes = numDlqBytes});
        }

        if (resultDoc) {
            curDataMsgByteSize += resultDoc->getApproximateSize();

            invariant(nextInputDocIdx > 0);
            const auto& streamDoc = dataMsg.docs[nextInputDocIdx - 1];
            StreamDocument resultStreamDoc(std::move(*resultDoc));
            resultStreamDoc.copyDocumentMetadata(streamDoc, _context);
            outputMsg.docs.emplace_back(std::move(resultStreamDoc));
            if (outputMsg.docs.size() == kDataMsgMaxDocSize ||
                curDataMsgByteSize >= kDataMsgMaxByteSize) {
                // Make sure to not wrap sendDataMsg() calls with a try/catch block.
                incOperatorStats({.timeSpent = dataMsg.creationTimer->elapsed()});
                sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg));
                outputMsg = newStreamDataMsg();
            }
        }
    }
    invariant(nextInputDocIdx == dataMsg.docs.size());

    if (!outputMsg.docs.empty()) {
        incOperatorStats({.timeSpent = dataMsg.creationTimer->elapsed()});
        sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg));
    }
    if (controlMsg) {
        onControlMsg(/*outputIdx*/ 0, std::move(*controlMsg));
    }
}

void UnwindOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    // This operator just passes through any control messages it sees.
    sendControlMsg(inputIdx, std::move(controlMsg));
}

}  // namespace streams
