#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $group.
 */
class GroupOperator : public DocumentSourceWrapperOperator {
public:
    GroupOperator(Context* context, DocumentSourceWrapperOperator::Options options)
        : DocumentSourceWrapperOperator(context, std::move(options)) {}

protected:
    std::string doGetName() const override {
        return "GroupOperator";
    }
};

}  // namespace streams
