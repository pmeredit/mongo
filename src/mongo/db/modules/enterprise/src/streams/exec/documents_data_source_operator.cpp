/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/documents_data_source_operator.h"
#include "mongo/db/basic_types.h"
#include "streams/exec/context.h"

using namespace mongo;

namespace streams {

std::vector<StreamMsgUnion> DocumentsDataSourceOperator::getMessages(WithLock) {
    StreamDataMsg dataMsg;
    dataMsg.docs.reserve(_options.docsPerRun);

    for (int docNum = 0; docNum < _options.docsPerRun && _documentIdx < _options.documents.size();
         ++docNum, ++_documentIdx) {
        dataMsg.docs.emplace_back(std::move(_options.documents[_documentIdx]));
    }

    if (dataMsg.docs.size()) {
        return {StreamMsgUnion{.dataMsg = std::move(dataMsg)}};
    } else {
        return {};
    }
}

}  // namespace streams
