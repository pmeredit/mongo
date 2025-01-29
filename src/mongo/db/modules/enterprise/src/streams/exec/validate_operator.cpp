/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/validate_operator.h"
#include "mongo/db/exec/matcher/matcher.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;

namespace streams {

ValidateOperator::ValidateOperator(Context* context, Options options)
    : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1), _options(std::move(options)) {}

void ValidateOperator::doOnDataMsg(int32_t inputIdx,
                                   StreamDataMsg dataMsg,
                                   boost::optional<StreamControlMsg> controlMsg) {
    StreamDataMsg outputMsg;
    outputMsg.docs.reserve(dataMsg.docs.size());
    int64_t numDlqDocs{0};
    int64_t numDlqBytes{0};
    for (auto& streamDoc : dataMsg.docs) {
        boost::optional<std::string> error;
        try {
            if (exec::matcher::matchesBSON(_options.validator.get(), streamDoc.doc.toBson())) {
                // Doc passed the validation.
                outputMsg.docs.emplace_back(std::move(streamDoc));
                continue;
            }
        } catch (const DBException& e) {
            error =
                str::stream() << "Failed to process input document in $validate stage with error: "
                              << e.what();
        }

        if (_options.validationAction == StreamsValidationActionEnum::Dlq) {
            if (!error) {
                error = "Input document found to be invalid in $validate stage";
            }
            numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(
                _context->streamMetaFieldName, streamDoc, getName(), std::move(error)));
            ++numDlqDocs;
        } else {
            // Else, discard the doc.
            dassert(_options.validationAction == StreamsValidationActionEnum::Discard);
        }
    }

    incOperatorStats({.numDlqDocs = numDlqDocs,
                      .numDlqBytes = numDlqBytes,
                      .timeSpent = dataMsg.creationTimer->elapsed()});
    sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), std::move(controlMsg));
}

void ValidateOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    // Simply forward the control message to the output.
    sendControlMsg(/*outputIdx*/ 0, std::move(controlMsg));
}

}  // namespace streams
