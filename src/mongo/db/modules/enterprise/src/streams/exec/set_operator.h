#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $set.
 */
class SetOperator : public DocumentSourceWrapperOperator {
public:
    SetOperator(mongo::DocumentSourceSingleDocumentTransformation* processor)
        : DocumentSourceWrapperOperator(processor) {}

protected:
    std::string doGetName() const override {
        return "SetOperator";
    }
};

}  // namespace streams
