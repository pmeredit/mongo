#include "streams/exec/document_source_wrapper_operator.h"
#include "mongo/db/pipeline/document_source.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include <optional>

namespace streams {

using namespace mongo;

void DocumentSourceWrapperOperator::doOnDataMsg(int32_t inputIdx,
                                                StreamDataMsg dataMsg,
                                                boost::optional<StreamControlMsg> controlMsg) {
    for (auto& doc : dataMsg.docs) {
        _feeder.addDocument(std::move(doc.doc));
    }

    StreamDataMsg outputMsg;
    auto result = _processor->getNext();
    while (result.isAdvanced()) {
        outputMsg.docs.emplace_back(result.releaseDocument());
        result = _processor->getNext();
    }

    this->sendDataMsg(0, std::move(outputMsg), std::move(controlMsg));
}

void DocumentSourceWrapperOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    // This operator just passes through any control messages it sees.
    this->sendControlMsg(inputIdx, std::move(controlMsg));
}

}  // namespace streams
