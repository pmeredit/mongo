/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/redact_operator.h"

#include "mongo/db/pipeline/document_source_redact.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/util.h"

namespace streams {

using namespace mongo;

RedactOperator::RedactOperator(Context* context, Options options)
    : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1),
      _options(std::move(options)),
      _processor(_options.documentSource->getRedactProcessor()) {}

void RedactOperator::doOnDataMsg(int32_t inputIdx,
                                 StreamDataMsg dataMsg,
                                 boost::optional<StreamControlMsg> controlMsg) {
    StreamDataMsg outputMsg;
    outputMsg.docs.reserve(dataMsg.docs.size());

    int64_t numDlqDocs{0};
    int64_t numDlqBytes{0};

    for (auto& streamDoc : dataMsg.docs) {
        boost::optional<Document> resultDoc;
        try {
            resultDoc = _processor->process(streamDoc.doc);
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            numDlqBytes += _context->dlq->addMessage(
                toDeadLetterQueueMsg(_context->streamMetaFieldName, streamDoc, std::move(error)));
            ++numDlqDocs;
        }

        if (!resultDoc) {
            continue;
        }
        streamDoc.doc = std::move(*resultDoc);
        outputMsg.docs.emplace_back(std::move(streamDoc));
    }
    incOperatorStats({.numDlqDocs = numDlqDocs, .numDlqBytes = numDlqBytes});

    // Make sure to not wrap sendDataMsg() calls with a try/catch block.
    sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), std::move(controlMsg));
}

void RedactOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    // This operator just passes through any control messages it sees.
    sendControlMsg(inputIdx, std::move(controlMsg));
}

}  // namespace streams
