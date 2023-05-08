#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $set.
 */
class SetOperator : public DocumentSourceWrapperOperator {
public:
    SetOperator(DocumentSourceWrapperOperator::Options options)
        : DocumentSourceWrapperOperator(std::move(options)) {}

protected:
    std::string doGetName() const override {
        return "SetOperator";
    }
};

}  // namespace streams
