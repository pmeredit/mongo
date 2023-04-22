/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/validate_operator.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/constants.h"
#include "streams/exec/dead_letter_queue.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

using namespace mongo;

namespace streams {

ValidateOperator::ValidateOperator(Options options)
    : Operator(/*numInputs*/ 1, /*numOutputs*/ 1), _options(std::move(options)) {}

void ValidateOperator::doOnDataMsg(int32_t inputIdx,
                                   StreamDataMsg dataMsg,
                                   boost::optional<StreamControlMsg> controlMsg) {
    StreamDataMsg outputMsg;
    outputMsg.docs.reserve(dataMsg.docs.size());
    for (auto& streamDoc : dataMsg.docs) {
        boost::optional<std::string> error;
        try {
            if (_options.validator->matchesBSON(streamDoc.doc.toBson())) {
                // Doc passed the validation.
                outputMsg.docs.emplace_back(std::move(streamDoc));
                continue;
            }
        } catch (const DBException& e) {
            error = e.what();
        }

        if (_options.validationAction == StreamsValidationActionEnum::Dlq) {
            _options.deadLetterQueue->addMessage(
                toDeadLetterQueueMsg(std::move(streamDoc), std::move(error)));
        }
        // Else, discard the doc.
        dassert(_options.validationAction == StreamsValidationActionEnum::Discard);
    }

    sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), std::move(controlMsg));
}

void ValidateOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    // Simply forward the control message to the output.
    sendControlMsg(/*outputIdx*/ 0, std::move(controlMsg));
}

BSONObjBuilder ValidateOperator::toDeadLetterQueueMsg(StreamDocument streamDoc,
                                                      boost::optional<std::string> error) {
    // TODO: Make sure that _stream_meta and hence kafka offsets get added to this message.
    BSONObjBuilder objBuilder;
    objBuilder.append("fullDocument", streamDoc.doc.toBson());
    BSONObj errInfo;
    if (error) {
        errInfo =
            BSON("reason" << str::stream()
                          << "Failed to process input document in $validate stage with error: "
                          << *error);
    } else {
        // TODO: Call doc_validation_error::generateError() like collection_impl.cpp does to get
        // the full error details.
        errInfo = BSON("reason"
                       << "Input document found to be invalid in $validate stage");
    }
    objBuilder.append("errInfo", std::move(errInfo));
    return objBuilder;
}

}  // namespace streams
