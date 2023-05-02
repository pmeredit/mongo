#include "streams/exec/document_source_wrapper_operator.h"
#include "mongo/db/pipeline/document_source.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include <optional>

namespace streams {

using namespace mongo;

DocumentSourceWrapperOperator::DocumentSourceWrapperOperator(mongo::DocumentSource* processor,
                                                             int32_t numOutputs)
    : Operator(1 /* numInputs */, numOutputs),
      _processor(processor),
      _feeder(processor->getContext()) {
    dassert(_numOutputs <= 1);
    _processor->setSource(&_feeder);
}

void DocumentSourceWrapperOperator::doOnDataMsg(int32_t inputIdx,
                                                StreamDataMsg dataMsg,
                                                boost::optional<StreamControlMsg> controlMsg) {
    StreamDataMsg outputMsg;
    outputMsg.docs.reserve(dataMsg.docs.size());

    for (auto& doc : dataMsg.docs) {
        _feeder.addDocument(std::move(doc.doc));

        auto result = _processor->getNext();
        while (result.isAdvanced()) {
            StreamDocument streamDoc(result.releaseDocument());
            streamDoc.copyDocumentMetadata(doc);
            outputMsg.docs.emplace_back(std::move(streamDoc));
            result = _processor->getNext();
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
