#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $project.
 */
class ProjectOperator : public DocumentSourceWrapperOperator {
public:
    ProjectOperator(mongo::DocumentSourceSingleDocumentTransformation* processor)
        : DocumentSourceWrapperOperator(processor) {}

protected:
    std::string doGetName() const override {
        return "ProjectOperator";
    }
};

}  // namespace streams
