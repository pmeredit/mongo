#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $redact.
 */
class RedactOperator : public DocumentSourceWrapperOperator {
public:
    RedactOperator(Context* context, DocumentSourceWrapperOperator::Options options)
        : DocumentSourceWrapperOperator(context, std::move(options)) {}

protected:
    std::string doGetName() const override {
        return "RedactOperator";
    }
};

}  // namespace streams
