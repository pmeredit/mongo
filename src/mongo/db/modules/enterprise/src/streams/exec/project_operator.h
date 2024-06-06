/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/single_document_transformation_operator.h"

namespace streams {

/**
 * The operator for $project.
 */
class ProjectOperator : public SingleDocumentTransformationOperator {
public:
    ProjectOperator(Context* context, SingleDocumentTransformationOperator::Options options)
        : SingleDocumentTransformationOperator(context, std::move(options)) {}

protected:
    std::string doGetName() const override {
        return "ProjectOperator";
    }
};

}  // namespace streams
