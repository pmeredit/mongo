#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/operator.h"

namespace streams {

/**
 * DocumentSourceWrapperOperator uses a DocumentSource instance for processing input and
 * producing output. This operator is used for stages like $match and $project.
 */
class DocumentSourceWrapperOperator : public Operator {

public:
    DocumentSourceWrapperOperator(mongo::DocumentSource* processor, int32_t numOutputs = 1);

    virtual ~DocumentSourceWrapperOperator() = default;

    mongo::DocumentSource& processor() {
        return *_processor;
    }

protected:
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

private:
    mongo::DocumentSource* _processor;
    DocumentSourceFeeder _feeder;
};

}  // namespace streams
