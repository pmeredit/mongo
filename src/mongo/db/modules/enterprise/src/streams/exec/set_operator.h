/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/single_document_transformation_operator.h"

namespace streams {

/**
 * The operator for $set.
 */
class SetOperator : public SingleDocumentTransformationOperator {
public:
    SetOperator(Context* context, SingleDocumentTransformationOperator::Options options)
        : SingleDocumentTransformationOperator(context, std::move(options)) {}

protected:
    std::string doGetName() const override {
        return "SetOperator";
    }
};

}  // namespace streams
