/**
 * Copyright (C) 2022-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "aggregate_expression_intender_entry.h"

namespace mongo {

namespace aggregate_expression_intender {

Intention mark(ExpressionContext* expCtx,
               const EncryptionSchemaTreeNode& schema,
               boost::intrusive_ptr<Expression>& expression,
               bool expressionOutputIsCompared,
               FLE2FieldRefExpr fieldRefSupported) {
    Intention finalIntention = Intention::NotMarked;

    if (schema.parsedFrom == FleVersion::kFle2) {
        finalIntention =
            markRange(expCtx, schema, expression, expressionOutputIsCompared, fieldRefSupported);
    }
    finalIntention =
        markEquality(
            expCtx, schema, expression.get(), expressionOutputIsCompared, fieldRefSupported) ||
        finalIntention;
    return finalIntention;
}

}  // namespace aggregate_expression_intender
}  // namespace mongo
