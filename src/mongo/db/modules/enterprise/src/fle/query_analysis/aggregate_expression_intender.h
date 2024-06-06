/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>

#include "agg_expression_encryption_intender_base.h"
#include "encryption_schema_tree.h"
#include "mongo/db/pipeline/expression.h"

namespace mongo {

namespace aggregate_expression_intender {

/**
 * Replace literals in the input query with intent-to-encrypt markings where needed. Throw
 * exceptions if a reference to an encrypted field occurs in a place where we need to take its value
 * for any purpose other than equality comparison to one or more literals or other encrypted fields
 * of like encryption type.
 *
 * Pass 'true' for 'expressionOutputIsCompared' if the owner of this Expression is going to use the
 * output to perform equality comparisons. For example, if this expression tree is used as the
 * group keys in a $group stage.
 *
 * The value of 'fieldRefSupported' determines if references to FLE 2-encrypted fields are allowed
 * within the expression. This value is ignored when 'schema' indicates we are using FLE 1.
 *
 * Returns an Intention enum indicating whether or not intent-to-encrypt markers were inserted.
 */
Intention markEquality(ExpressionContext* expCtx,
                       const EncryptionSchemaTreeNode& schema,
                       Expression* expression,
                       bool expressionOutputIsCompared,
                       FLE2FieldRefExpr fieldRefSupported = FLE2FieldRefExpr::disallowed);

/**
 * Given an input 'expression' and 'schema', returns the output schema associated with the evaluated
 * result of the expression. This method does *not* check for semantically invalid operations on
 * encrypted fields.
 *
 * For instance, if the input 'schema' indicates that the path 'a' is encrypted, then calling this
 * method with the expression '$a' would result in a pointer to a schema tree with a single
 * EncryptionSchemaEncryptedNode at the root.
 *
 * Pass 'true' for 'expressionOutputIsCompared' if the owner of this Expression is going to use the
 * output to perform equality comparisons. For example, if this expression tree is used as the
 * group keys in a $group stage.
 *
 * Throws an assertion if the schema of the evaluated result cannot be statically determined at
 * parse time or if the expression is invalid for client-side encryption.
 */
std::unique_ptr<EncryptionSchemaTreeNode> getOutputSchema(const EncryptionSchemaTreeNode& schema,
                                                          Expression* expression,
                                                          bool expressionOutputIsCompared);

}  // namespace aggregate_expression_intender
}  // namespace mongo
