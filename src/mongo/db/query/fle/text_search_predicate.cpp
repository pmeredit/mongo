/**
 *    Copyright (C) 2025-present MongoDB, Inc.
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

#include "text_search_predicate.h"

namespace mongo::fle {

REGISTER_ENCRYPTED_AGG_PREDICATE_REWRITE(ExpressionEncStrStartsWith, TextSearchPredicate);

std::vector<PrfBlock> TextSearchPredicate::generateTags(BSONValue payload) const {
    // TODO SERVER-101128: Update to generate the correct tags.
    return {};
}

std::unique_ptr<Expression> TextSearchPredicate::rewriteToTagDisjunction(Expression* expr) const {
    if (auto encStrStartsWithExpr = dynamic_cast<ExpressionEncStrStartsWith*>(expr);
        encStrStartsWithExpr) {
        const auto& textConstant = encStrStartsWithExpr->getText();
        auto payload = textConstant.getValue();
        if (!isPayload(payload)) {
            return nullptr;
        }
        return makeTagDisjunction(_rewriter->getExpressionContext(),
                                  toValues(generateTags(std::ref(payload))));
    }
    MONGO_UNREACHABLE_TASSERT(10112602);
}

std::unique_ptr<Expression> TextSearchPredicate::rewriteToRuntimeComparison(
    Expression* expr) const {
    if (auto encStrStartsWithExpr = dynamic_cast<ExpressionEncStrStartsWith*>(expr);
        encStrStartsWithExpr) {
        // Since we don't support non-encrypted ExpressionEncStrStartsWith,
        // ExpressionEncStrStartsWith is already considered the runtime comparison, therefore we
        // don't need to do rewriting here.
        return nullptr;
    }
    MONGO_UNREACHABLE_TASSERT(10112603);
}

std::unique_ptr<MatchExpression> TextSearchPredicate::rewriteToTagDisjunction(
    MatchExpression* expr) const {
    tasserted(10112600,
              "Encrypted text search predicates are only supported as aggregation expressions.");
}

std::unique_ptr<MatchExpression> TextSearchPredicate::rewriteToRuntimeComparison(
    MatchExpression* expr) const {
    tasserted(10112601,
              "Encrypted text search predicates are only supported as aggregation expressions.");
}

}  // namespace mongo::fle
