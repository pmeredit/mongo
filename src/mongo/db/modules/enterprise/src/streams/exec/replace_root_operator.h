#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $replaceRoot.
 */
class ReplaceRootOperator : public DocumentSourceWrapperOperator {
public:
    ReplaceRootOperator(mongo::DocumentSourceSingleDocumentTransformation* processor)
        : DocumentSourceWrapperOperator(processor) {}

protected:
    std::string doGetName() const override {
        return "ReplaceRootOperator";
    }
};

}  // namespace streams
