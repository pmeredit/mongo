#pragma once

#include "streams/exec/document_source_wrapper_operator.h"

namespace streams {

/**
 * The operator for $addFields.
 */
class AddFieldsOperator : public DocumentSourceWrapperOperator {
public:
    AddFieldsOperator(mongo::DocumentSource* processor)
        : DocumentSourceWrapperOperator(processor) {}

protected:
    std::string doGetName() const override {
        return "AddFieldsOperator";
    }
};

}  // namespace streams
