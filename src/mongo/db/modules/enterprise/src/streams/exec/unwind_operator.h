#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $unwind.
 */
class UnwindOperator : public DocumentSourceWrapperOperator {
public:
    UnwindOperator(DocumentSourceWrapperOperator::Options options)
        : DocumentSourceWrapperOperator(std::move(options)) {}

protected:
    std::string doGetName() const override {
        return "UnwindOperator";
    }
};

}  // namespace streams
