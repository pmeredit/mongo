#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/sink_operator.h"

namespace mongo {
class DocumentSourceMerge;
}

namespace streams {

struct Context;

/**
 * The operator for $merge.
 */
// TODO: DocumentSourceMerge does internal buffering. We may want to explicitly flush in some cases
// or have a timeout on this buffering.
class MergeOperator : public SinkOperator {
public:
    struct Options {
        // Execution context.
        Context* context{nullptr};
        // DocumentSource stage that this Operator wraps.
        mongo::DocumentSource* processor;
    };

    MergeOperator(Options options);

    mongo::DocumentSource& processor() {
        return *_options.processor;
    }

protected:
    std::string doGetName() const override {
        return "MergeOperator";
    }

    void doSinkOnDataMsg(int32_t inputIdx,
                         StreamDataMsg dataMsg,
                         boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override {
        // This operator simply eats any control messages it receives.
    }

private:
    Options _options;
    DocumentSourceFeeder _feeder;
};

}  // namespace streams
