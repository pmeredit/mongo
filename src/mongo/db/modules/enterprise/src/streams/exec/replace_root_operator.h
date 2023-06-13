#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $replaceRoot.
 */
class ReplaceRootOperator : public DocumentSourceWrapperOperator {
public:
    ReplaceRootOperator(Context* context, DocumentSourceWrapperOperator::Options options)
        : DocumentSourceWrapperOperator(context, std::move(options)) {}

protected:
    std::string doGetName() const override {
        return "ReplaceRootOperator";
    }
};

}  // namespace streams
