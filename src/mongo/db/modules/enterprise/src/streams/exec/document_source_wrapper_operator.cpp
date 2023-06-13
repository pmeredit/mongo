/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <optional>

#include "mongo/db/pipeline/document_source.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/document_source_wrapper_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/util.h"

namespace streams {

using namespace mongo;

DocumentSourceWrapperOperator::DocumentSourceWrapperOperator(Context* context, Options options)
    : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1),
      _options(std::move(options)),
      _feeder(_options.processor->getContext()) {
    dassert(_numOutputs <= 1);
    _options.processor->setSource(&_feeder);
}

void DocumentSourceWrapperOperator::doOnDataMsg(int32_t inputIdx,
                                                StreamDataMsg dataMsg,
                                                boost::optional<StreamControlMsg> controlMsg) {
    StreamDataMsg outputMsg;
    outputMsg.docs.reserve(dataMsg.docs.size());

    auto processStreamDoc = [&](StreamDocument& streamDoc) {
        _feeder.addDocument(std::move(streamDoc.doc));

        auto result = _options.processor->getNext();
        while (result.isAdvanced()) {
            StreamDocument resultStreamDoc(result.releaseDocument());
            resultStreamDoc.copyDocumentMetadata(streamDoc);
            outputMsg.docs.emplace_back(std::move(resultStreamDoc));
            result = _options.processor->getNext();
        }
    };

    for (auto& streamDoc : dataMsg.docs) {
        try {
            processStreamDoc(streamDoc);
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            _context->dlq->addMessage(toDeadLetterQueueMsg(streamDoc.streamMeta, std::move(error)));
        }
    }

    if (_numOutputs != 0) {
        sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), std::move(controlMsg));
    }
}

void DocumentSourceWrapperOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    if (_numOutputs != 0) {
        // This operator just passes through any control messages it sees.
        sendControlMsg(inputIdx, std::move(controlMsg));
    }
}

}  // namespace streams
