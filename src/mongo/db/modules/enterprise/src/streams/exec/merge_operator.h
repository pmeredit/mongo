#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $merge.
 */
// TODO: DocumentSourceMerge does internal buffering. We may want to explicitly flush in some cases
// or have a timeout on this buffering.
class MergeOperator : public DocumentSourceWrapperOperator {
public:
    MergeOperator(mongo::DocumentSourceMerge* processor)
        : DocumentSourceWrapperOperator(processor, /*numOutputs*/ 0) {}

protected:
    std::string doGetName() const override {
        return "MergeOperator";
    }
};

}  // namespace streams
