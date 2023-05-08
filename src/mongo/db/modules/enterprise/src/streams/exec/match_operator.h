#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $match.
 */
class MatchOperator : public DocumentSourceWrapperOperator {
public:
    MatchOperator(DocumentSourceWrapperOperator::Options options)
        : DocumentSourceWrapperOperator(std::move(options)) {}

protected:
    std::string doGetName() const override {
        return "MatchOperator";
    }
};

}  // namespace streams
