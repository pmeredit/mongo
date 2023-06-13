#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/operator.h"

namespace streams {

struct Context;

/**
 * DocumentSourceWrapperOperator uses a DocumentSource instance for processing input and
 * producing output. This operator is used for stages like $match and $project.
 */
class DocumentSourceWrapperOperator : public Operator {
public:
    struct Options {
        // DocumentSource stage that this Operator wraps.
        mongo::DocumentSource* processor;
    };

    DocumentSourceWrapperOperator(Context* context, Options options);

    virtual ~DocumentSourceWrapperOperator() = default;

    mongo::DocumentSource& processor() {
        return *_options.processor;
    }

protected:
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

private:
    Options _options;
    DocumentSourceFeeder _feeder;
};

}  // namespace streams
