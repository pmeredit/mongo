/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/document_source_merge.h"
#include "streams/exec/merge_operator.h"

namespace streams {

using namespace mongo;

MergeOperator::MergeOperator(mongo::DocumentSourceMerge* processor)
    : SinkOperator(1 /* numInputs */), _processor(processor), _feeder(processor->getContext()) {
    _processor->setSource(&_feeder);
}

void MergeOperator::doOnDataMsg(int32_t inputIdx,
                                StreamDataMsg dataMsg,
                                boost::optional<StreamControlMsg> controlMsg) {
    sendOutputToSamplers(dataMsg);

    for (auto& doc : dataMsg.docs) {
        _feeder.addDocument(std::move(doc.doc));
    }

    StreamDataMsg outputMsg;
    auto result = _processor->getNext();
    while (result.isAdvanced()) {
        outputMsg.docs.emplace_back(result.releaseDocument());
        result = _processor->getNext();
    }
}


}  // namespace streams
