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
    IntentionPreVisitor(ExpressionContext* expCtx,
                        const EncryptionSchemaTreeNode& schema,
                        std::stack<Subtree>& subtreeStack,
                        FLE2FieldRefExpr fieldRefSupported)
        : mongo::aggregate_expression_intender::IntentionPreVisitorBase(
              expCtx, schema, subtreeStack, fieldRefSupported) {}

protected:
    using mongo::aggregate_expression_intender::IntentionPreVisitorBase::visit;
    void visit(ExpressionCompare* compare) final {
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
            default:
                // This walker only considers equality comparisons. For other operators they could
                // be encrypted placeholders from previous walker passes or regular queries.
                // Both these cases must be handled by all walkers.
                IntentionPreVisitorBase::visit(compare);
                return;
        }
    }

private:
    FLE2FieldRefExpr fieldRefSupported;
};


class EqualityIntentionWalker final : public AggExprEncryptionIntentionWalkerBase {
public:
    EqualityIntentionWalker(ExpressionContext* expCtx,
                            const EncryptionSchemaTreeNode& schema,
                            bool expressionOutputIsCompared,
                            FLE2FieldRefExpr fieldRefSupported)
        : AggExprEncryptionIntentionWalkerBase(expCtx, schema, expressionOutputIsCompared),
          fieldRefSupported(fieldRefSupported) {}


private:
    IntentionPreVisitorBase* getPreVisitor() override {
        return &intentionPreVisitor;
    }
    IntentionInVisitorBase* getInVisitor() override {
        return &intentionInVisitor;
    }
    IntentionPostVisitorBase* getPostVisitor() override {
        return &intentionPostVisitor;
    }

    FLE2FieldRefExpr fieldRefSupported;

    IntentionPreVisitor intentionPreVisitor{expCtx, schema, subtreeStack, fieldRefSupported};
    IntentionInVisitorBase intentionInVisitor{*expCtx, schema, subtreeStack};
    IntentionPostVisitorBase intentionPostVisitor{*expCtx, schema, subtreeStack};
};

}  // namespace

Intention markEquality(ExpressionContext* expCtx,
                       const EncryptionSchemaTreeNode& schema,
                       Expression* expression,
                       bool expressionOutputIsCompared,
                       FLE2FieldRefExpr fieldRefSupported) {
    EqualityIntentionWalker walker{expCtx, schema, expressionOutputIsCompared, fieldRefSupported};
    expression_walker::walk<Expression>(expression, &walker);
    return walker.exitOutermostSubtree(expressionOutputIsCompared);
}

}  // namespace mongo::aggregate_expression_intender
