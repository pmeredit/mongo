/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <boost/optional.hpp>

#include "encryption_schema_tree.h"
#include "mongo/db/pipeline/expression.h"

namespace mongo::aggregate_expression_intender {

/**
 * Indicates whether or not mark() actually inserted any intent-to-encrypt markers, since they are
 * not always necessary.
 */
enum class [[nodiscard]] Intention : bool{Marked = true, NotMarked = false};

inline Intention operator||(Intention a, Intention b) {
    if (a == Intention::Marked || b == Intention::Marked) {
        return Intention::Marked;
    } else {
        return Intention::NotMarked;
    }
}
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
 * Returns an Intention enum indicating whether or not intent-to-encrypt markers were inserted.
 */
Intention mark(const ExpressionContext& expCtx,
               const EncryptionSchemaTreeNode& schema,
               Expression* expression,
               bool expressionOutputIsCompared);

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

}  // namespace mongo::aggregate_expression_intender
