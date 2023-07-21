/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/single_document_transformation_operator.h"

#include "mongo/db/pipeline/document_source_single_document_transformation.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/util.h"

namespace streams {

using namespace mongo;

SingleDocumentTransformationOperator::SingleDocumentTransformationOperator(Context* context,
                                                                           Options options)
    : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1),
      _options(std::move(options)),
      _processor(_options.documentSource->getTransformationProcessor()) {}

void SingleDocumentTransformationOperator::doOnDataMsg(
    int32_t inputIdx, StreamDataMsg dataMsg, boost::optional<StreamControlMsg> controlMsg) {
    StreamDataMsg outputMsg;
    outputMsg.docs.reserve(dataMsg.docs.size());

    for (auto& streamDoc : dataMsg.docs) {
        try {
            auto resultDoc = _processor->process(streamDoc.doc);
            streamDoc.doc = std::move(resultDoc);
            outputMsg.docs.emplace_back(std::move(streamDoc));
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            _context->dlq->addMessage(toDeadLetterQueueMsg(streamDoc.streamMeta, std::move(error)));
        }
    }

    // Make sure to not wrap sendDataMsg() calls with a try/catch block.
    sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), std::move(controlMsg));
}

void SingleDocumentTransformationOperator::doOnControlMsg(int32_t inputIdx,
                                                          StreamControlMsg controlMsg) {
    // This operator just passes through any control messages it sees.
    sendControlMsg(inputIdx, std::move(controlMsg));
}

}  // namespace streams
