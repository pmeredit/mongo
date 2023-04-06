#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $match.
 */
class MatchOperator : public DocumentSourceWrapperOperator {
public:
    MatchOperator(mongo::DocumentSourceMatch* processor)
        : DocumentSourceWrapperOperator(processor) {}

protected:
    std::string doGetName() const override {
        return "MatchOperator";
    }
};

}  // namespace streams
