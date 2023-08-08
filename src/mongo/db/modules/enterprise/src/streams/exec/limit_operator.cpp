/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/limit_operator.h"

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/context.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

void LimitOperator::doOnDataMsg(int32_t inputIdx,
                                StreamDataMsg dataMsg,
                                boost::optional<StreamControlMsg> controlMsg) {
    // Let any exceptions that occur here escape upstream.
    invariant(_limit >= _numSent);
    auto& inputDocs = dataMsg.docs;
    size_t numDocsToSend = std::min<size_t>(_limit - _numSent, inputDocs.size());
    _numSent += numDocsToSend;
    invariant(_limit >= _numSent);
    if (numDocsToSend < inputDocs.size()) {
        inputDocs.erase(inputDocs.begin() + numDocsToSend, inputDocs.end());
    }
    invariant(numDocsToSend == inputDocs.size());

    if (!inputDocs.empty()) {
        sendDataMsg(/*outputIdx*/ 0, std::move(dataMsg), std::move(controlMsg));
    } else if (controlMsg) {
        sendControlMsg(/*outputIdx*/ 0, std::move(*controlMsg));
    }
}

void LimitOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    // This operator just passes through any control messages it sees.
    sendControlMsg(inputIdx, std::move(controlMsg));
}

}  // namespace streams
