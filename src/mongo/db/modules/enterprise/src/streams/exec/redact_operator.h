#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $redact.
 */
class RedactOperator : public DocumentSourceWrapperOperator {
public:
    RedactOperator(mongo::DocumentSourceRedact* processor)
        : DocumentSourceWrapperOperator(processor) {}

protected:
    std::string doGetName() const override {
        return "RedactOperator";
    }
};

}  // namespace streams
