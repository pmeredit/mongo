#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $replaceRoot.
 */
class ReplaceRootOperator : public DocumentSourceWrapperOperator {
public:
    ReplaceRootOperator(DocumentSourceWrapperOperator::Options options)
        : DocumentSourceWrapperOperator(std::move(options)) {}

protected:
    std::string doGetName() const override {
        return "ReplaceRootOperator";
    }
};

}  // namespace streams
