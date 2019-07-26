/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
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
