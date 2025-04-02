/**
 * Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "agg_expression_encryption_intender_base.h"
#include "encryption_schema_tree.h"
#include "mongo/db/pipeline/expression.h"

namespace mongo {

namespace aggregate_expression_intender {

/**
 * Replace literals in the input query with intent-to-encrypt markings for text search expressions.
 * Marks encrypted field paths for literals that should be replaced by markRange() or
 * markEquality(), but does not create any placeholders for those literals.
 *
 * Pass 'true' for 'expressionOutputIsCompared' if the owner of this Expression is going to use the
 * output to perform equality comparisons. For example, if this expression tree is used as the
 * group keys in a $group stage.
 *
 * The value of 'fieldRefSupported' determines if references to FLE 2-encrypted fields are allowed
 * within the expression. This value is ignored when 'schema' indicates we are using FLE 1.
 *
 * Returns an Intention enum indicating whether or not intent-to-encrypt markers were
 * inserted.
 */
Intention markTextSearch(ExpressionContext* expCtx,
                         const EncryptionSchemaTreeNode& schema,
                         boost::intrusive_ptr<Expression>& expression,
                         bool expressionOutputIsCompared,
                         FLE2FieldRefExpr fieldRefSupported = FLE2FieldRefExpr::allowed);


}  // namespace aggregate_expression_intender
}  // namespace mongo
