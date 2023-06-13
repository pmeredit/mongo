/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "streams/exec/merge_operator.h"

#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/util.h"

namespace streams {

using namespace mongo;

MergeOperator::MergeOperator(Context* context, Options options)
    : SinkOperator(context, 1 /* numInputs */),
      _options(std::move(options)),
      _feeder(_options.processor->getContext()) {
    _options.processor->setSource(&_feeder);
}

void MergeOperator::doSinkOnDataMsg(int32_t inputIdx,
                                    StreamDataMsg dataMsg,
                                    boost::optional<StreamControlMsg> controlMsg) {
    auto processStreamDoc = [&](StreamDocument& streamDoc) {
        _feeder.addDocument(std::move(streamDoc.doc));

        auto result = _options.processor->getNext();
        uassert(ErrorCodes::InternalError,
                str::stream() << "unexpected result state",
                !result.isAdvanced());
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
}

}  // namespace streams
