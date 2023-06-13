#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $project.
 */
class ProjectOperator : public DocumentSourceWrapperOperator {
public:
    ProjectOperator(Context* context, DocumentSourceWrapperOperator::Options options)
        : DocumentSourceWrapperOperator(context, std::move(options)) {}

protected:
    std::string doGetName() const override {
        return "ProjectOperator";
    }
};

}  // namespace streams
