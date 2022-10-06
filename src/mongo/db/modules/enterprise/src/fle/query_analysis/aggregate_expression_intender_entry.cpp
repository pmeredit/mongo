/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
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
    if (gFeatureFlagFLE2Range.isEnabledAndIgnoreFCV() && schema.parsedFrom == FleVersion::kFle2) {
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
