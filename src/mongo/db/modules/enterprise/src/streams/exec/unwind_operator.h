#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $unwind.
 */
class UnwindOperator : public DocumentSourceWrapperOperator {
public:
    UnwindOperator(mongo::DocumentSourceUnwind* processor)
        : DocumentSourceWrapperOperator(processor) {}

protected:
    std::string doGetName() const override {
        return "UnwindOperator";
    }
};

}  // namespace streams
