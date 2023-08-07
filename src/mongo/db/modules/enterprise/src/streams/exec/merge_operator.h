#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "streams/exec/sink_operator.h"

namespace mongo {
class DocumentSourceMerge;
class MergeProcessor;
}  // namespace mongo

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
        // DocumentSource stage that this Operator wraps.
        mongo::DocumentSourceMerge* documentSource;
    };

    MergeOperator(Context* context, Options options);

    mongo::DocumentSourceMerge* documentSource() {
        return _options.documentSource;
    }

protected:
    std::string doGetName() const override {
        return "MergeOperator";
    }

    void doSinkOnDataMsg(int32_t inputIdx,
                         StreamDataMsg dataMsg,
                         boost::optional<StreamControlMsg> controlMsg) override;

private:
    // Processes the docs at the indexes [startIdx, endIdx) in 'dataMsg'.
    void processStreamDocs(const StreamDataMsg& dataMsg,
                           size_t startIdx,
                           size_t endIdx,
                           size_t maxBatchDocSize);

    Options _options;
    mongo::MergeProcessor* _processor{nullptr};
};

}  // namespace streams
