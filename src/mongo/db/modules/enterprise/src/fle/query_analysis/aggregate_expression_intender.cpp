/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <fmt/format.h>
#include <functional>
#include <numeric>
#include <stack>
#include <string>
#include <type_traits>
#include <utility>

#include "aggregate_expression_intender.h"
#include "mongo/base/string_data.h"
#include "mongo/db/pipeline/expression_visitor.h"
#include "mongo/db/pipeline/expression_walker.h"
#include "mongo/stdx/variant.h"
#include "query_analysis.h"
#include "resolved_encryption_info.h"

namespace mongo::aggregate_expression_intender {

namespace {

using namespace fmt::literals;
using namespace std::string_literals;


/**
 * We prefer front-loading work and doing as much as possible in the PreVisitor for
 * organization.
 */
class IntentionPreVisitor final : public IntentionPreVisitorBase {
public:
    IntentionPreVisitor(const ExpressionContext& expCtx,
                        const EncryptionSchemaTreeNode& schema,
                        std::stack<Subtree>& subtreeStack,
                        FLE2FieldRefExpr fieldRefSupported)
        : mongo::aggregate_expression_intender::IntentionPreVisitorBase(
              expCtx, schema, subtreeStack),
          fieldRefSupported(fieldRefSupported) {}

protected:
    using mongo::aggregate_expression_intender::IntentionPreVisitorBase::visit;
    virtual void visit(ExpressionCompare* compare) override final {
        switch (compare->getOp()) {
            case ExpressionCompare::EQ:
            case ExpressionCompare::NE: {
                // The result of this comparison will be either true or false, never encrypted. So
                // if the Subtree above us is comparing to an encrypted value that has to be an
                // error.
                ensureNotEncrypted("an equality comparison", subtreeStack);
                // Now that we're sure our result won't be compared to encrypted values, enter a new
                // Subtree to provide a new context for our children - this is a fresh start.
                Subtree::Compared comparedSubtree;

                // We specifically support references to encrypted field paths under $eq, e.g.
                // {$eq: ["$encryptedField", <constant>]}. If there is exactly one field path as a
                // child to this $eq, set temporarilyPermittedEncryptedFieldPath to indicate that
                // the field path is allowed.
                if (schema.parsedFrom == FleVersion::kFle2) {
                    auto firstChild = compare->getOperandList()[0].get();
                    auto firstOpFieldPath = dynamic_cast<ExpressionFieldPath*>(firstChild);
                    auto secondChild = compare->getOperandList()[1].get();
                    auto secondOpFieldPath = dynamic_cast<ExpressionFieldPath*>(secondChild);

                    auto isEncryptedFieldPath = [&](ExpressionFieldPath* fieldPathExpr) {
                        if (fieldPathExpr) {
                            auto ref = fieldPathExpr->getFieldPathWithoutCurrentPrefix().fullPath();
                            return schema.getEncryptionMetadataForPath(FieldRef{ref}) ||
                                schema.mayContainEncryptedNodeBelowPrefix(FieldRef{ref});
                        }
                        return false;
                    };
                    auto firstEncrypted = isEncryptedFieldPath(firstOpFieldPath);
                    auto secondEncrypted = isEncryptedFieldPath(secondOpFieldPath);

                    if (firstEncrypted && secondEncrypted) {
                        uassertedComparisonFLE2EncryptedFields(
                            firstOpFieldPath->getFieldPathWithoutCurrentPrefix(),
                            secondOpFieldPath->getFieldPathWithoutCurrentPrefix());
                    } else if (firstEncrypted) {
                        comparedSubtree.temporarilyPermittedEncryptedFieldPath = firstOpFieldPath;
                        ensureFLE2EncryptedFieldComparedToConstant(firstOpFieldPath, secondChild);
                    } else if (secondEncrypted) {
                        comparedSubtree.temporarilyPermittedEncryptedFieldPath = secondOpFieldPath;
                        ensureFLE2EncryptedFieldComparedToConstant(secondOpFieldPath, firstChild);
                    }
                }

                enterSubtree(comparedSubtree, subtreeStack);
                return;
            }
            case ExpressionCompare::GT:
                ensureNotEncryptedEnterEval("a greater than comparison", subtreeStack);
                return;
            case ExpressionCompare::GTE:
                ensureNotEncryptedEnterEval("a greater than or equal comparison", subtreeStack);
                return;
            case ExpressionCompare::LT:
                ensureNotEncryptedEnterEval("a less than comparison", subtreeStack);
                return;
            case ExpressionCompare::LTE:
                ensureNotEncryptedEnterEval("a less than or equal comparison", subtreeStack);
                return;
            case ExpressionCompare::CMP:
                ensureNotEncryptedEnterEval("a three-way comparison", subtreeStack);
                return;
        }
    }
    virtual void visit(ExpressionFieldPath* fieldPath) override final {
        // Variables are handled with seperate logic from field references.
        if (fieldPath->getFieldPath().getFieldName(0) != "CURRENT" ||
            fieldPath->getFieldPath().getPathLength() <= 1) {
            reconcileVariableAccess(*fieldPath, subtreeStack);
        } else {
            FieldRef path{fieldPath->getFieldPathWithoutCurrentPrefix().fullPath()};
            // Most of the time it is illegal to use a FieldPath in an aggregation expression.
            // However, there are certain exceptions. To determine whether a FieldPath is allowed in
            // the current context we must examine the Subtree stack and check if a previously
            // visited expression determined it was ok.
            if (auto comparedSubtree = stdx::get_if<Subtree::Compared>(&subtreeStack.top().output);
                fieldRefSupported == FLE2FieldRefExpr::allowed && comparedSubtree &&
                fieldPath == comparedSubtree->temporarilyPermittedEncryptedFieldPath) {
                comparedSubtree->temporarilyPermittedEncryptedFieldPath = nullptr;
            } else {
                FieldRef path{fieldPath->getFieldPathWithoutCurrentPrefix().fullPath()};
                uassert(6331102,
                        "Invalid reference to an encrypted field within aggregate expression: "s +
                            path.dottedField(),
                        (!schema.getEncryptionMetadataForPath(path) &&
                         !schema.mayContainEncryptedNodeBelowPrefix(path)) ||
                            schema.parsedFrom == FleVersion::kFle1);
            }

            attemptReconcilingFieldEncryption(schema, *fieldPath, subtreeStack);
            // Indicate that we've seen this field to improve error messages if we see an
            // incompatible field later.
            if (auto compared = stdx::get_if<Subtree::Compared>(&subtreeStack.top().output))
                compared->fields.push_back(fieldPath->getFieldPathWithoutCurrentPrefix());
        }
    }

private:
    FLE2FieldRefExpr fieldRefSupported;
};


class EqualityIntentionWalker final : public AggExprEncryptionIntentionWalkerBase {
public:
    EqualityIntentionWalker(const ExpressionContext& expCtx,
                            const EncryptionSchemaTreeNode& schema,
                            bool expressionOutputIsCompared,
                            FLE2FieldRefExpr fieldRefSupported)
        : AggExprEncryptionIntentionWalkerBase(expCtx, schema, expressionOutputIsCompared),
          fieldRefSupported(fieldRefSupported) {}


private:
    virtual IntentionPreVisitorBase* getPreVisitor() override {
        return &intentionPreVisitor;
    }

    FLE2FieldRefExpr fieldRefSupported;

    IntentionPreVisitor intentionPreVisitor{expCtx, schema, subtreeStack, fieldRefSupported};
};

}  // namespace

Intention mark(const ExpressionContext& expCtx,
               const EncryptionSchemaTreeNode& schema,
               Expression* expression,
               bool expressionOutputIsCompared,
               FLE2FieldRefExpr fieldRefSupported) {
    EqualityIntentionWalker walker{expCtx, schema, expressionOutputIsCompared, fieldRefSupported};
    expression_walker::walk<Expression>(expression, &walker);
    return walker.exitOutermostSubtree(expressionOutputIsCompared);
}

}  // namespace mongo::aggregate_expression_intender
