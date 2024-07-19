/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/match_operator.h"

#include "mongo/db/pipeline/document_source_match.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/util.h"

// If enabled the executor thread will sleep for some time after processing a batch of fetched
// events
MONGO_FAIL_POINT_DEFINE(matchOperatorSlowEventProcessing);

namespace streams {

using namespace mongo;

MatchOperator::MatchOperator(Context* context, Options options)
    : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1),
      _options(std::move(options)),
      _processor(_options.documentSource->getMatchProcessor()) {}

void MatchOperator::doOnDataMsg(int32_t inputIdx,
                                StreamDataMsg dataMsg,
                                boost::optional<StreamControlMsg> controlMsg) {
    StreamDataMsg outputMsg;
    outputMsg.docs.reserve(dataMsg.docs.size());

    int64_t numDlqDocs{0};
    int64_t numDlqBytes{0};

    if (MONGO_unlikely(matchOperatorSlowEventProcessing.shouldFail())) {
        sleepFor(Milliseconds{2500});
    }

    for (auto& streamDoc : dataMsg.docs) {
        bool matchResult{false};
        try {
            matchResult = _processor->process(streamDoc.doc);
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            numDlqBytes += _context->dlq->addMessage(
                toDeadLetterQueueMsg(_context->streamMetaFieldName, streamDoc, std::move(error)));
            ++numDlqDocs;
        }

        if (matchResult) {
            outputMsg.docs.emplace_back(std::move(streamDoc));
        }
    }

    incOperatorStats({.numDlqDocs = numDlqDocs,
                      .numDlqBytes = numDlqBytes,
                      .timeSpent = dataMsg.creationTimer->elapsed()});

    // Make sure to not wrap sendDataMsg() calls with a try/catch block.
    sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), std::move(controlMsg));
}

void MatchOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    // This operator just passes through any control messages it sees.
    sendControlMsg(inputIdx, std::move(controlMsg));
}

}  // namespace streams
