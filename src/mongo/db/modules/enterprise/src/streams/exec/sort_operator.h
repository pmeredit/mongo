#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $sort.
 */
class SortOperator : public DocumentSourceWrapperOperator {
public:
    SortOperator(Context* context, DocumentSourceWrapperOperator::Options options)
        : DocumentSourceWrapperOperator(context, std::move(options)) {}

protected:
    std::string doGetName() const override {
        return "SortOperator";
    }
};

}  // namespace streams
