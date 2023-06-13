#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $unwind.
 */
class UnwindOperator : public DocumentSourceWrapperOperator {
public:
    UnwindOperator(Context* context, DocumentSourceWrapperOperator::Options options)
        : DocumentSourceWrapperOperator(context, std::move(options)) {}

protected:
    std::string doGetName() const override {
        return "UnwindOperator";
    }
};

}  // namespace streams
