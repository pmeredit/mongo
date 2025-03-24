/**
 * Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "fle/query_analysis/agg_expression_encryption_intender_base.h"
#include "mongo/platform/basic.h"

#include "aggregate_expression_intender.h"
#include "mongo/base/string_data.h"
#include "mongo/db/pipeline/expression_visitor.h"
#include "mongo/db/pipeline/expression_walker.h"
#include "query_analysis.h"
#include <climits>
#include <fmt/format.h>

namespace mongo::aggregate_expression_intender {

namespace {

using namespace std::string_literals;

/**
 * We prefer front-loading work and doing as much as possible in the PreVisitor for
 * organization.
 *
 * This IntentionPreVisitor must set permitted encrypted field paths for non text search expressions
 * (ex. ExpressionCompare*) similar to the base implementation as there is a condition check for
 * ExpressionFieldPaths during the PostVisit. These literals will not be replaced with placeholders
 * as the text IntentionPostVisitor will only perform replacements of literals to placeholders for
 * text predicates.
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
        // $cmp is never encrypted. Enter an Eval subtree immediately instead of descending into
        // a comparison that allows encryption.
        if (compare->getOp() == ExpressionCompare::CMP) {
            ensureNotEncryptedEnterEval("a three-way comparison", subtreeStack);
            return;
        }

        // The result of this comparison will be either true or false, never encrypted. So
        // if the Subtree above us is comparing to an encrypted value that has to be an
        // error.
        ensureNotEncrypted("a comparison", subtreeStack);

        // Now that we're sure our result won't be compared to encrypted values, enter a new
        // Subtree to provide a new context for our children - this is a fresh start.
        Subtree::Compared comparedSubtree;

        // If there is an encrypted fieldPath, we mark it as a valid encrypted field path. Although
        // we won't replace comparison literals with placeholders, we must correctly mark encrypted
        // field paths so we don't error when coming across encrypted fieldpaths during the post
        // visit.
        auto [relevantPath, relevantConstant] = getFieldPathAndConstantFromExpression(compare);
        if (!isEncryptedFieldPath(relevantPath)) {
            // Enter the compared subtree but don't allow any encryption.
            enterSubtree(comparedSubtree, subtreeStack);
            return;
        }

        ensureFLE2EncryptedFieldComparedToConstant(relevantPath, relevantConstant);
        comparedSubtree.temporarilyPermittedEncryptedFieldPath = relevantPath;
        enterSubtree(comparedSubtree, subtreeStack);
    }

    void visit(ExpressionEncStrStartsWith* encStrStartsWith) final {
        uassert(10112203,
                "$encStrStartsWith can only be used with FLE2",
                schema.parsedFrom == FleVersion::kFle2);

        tassert(10112202,
                "$encStrStartsWith encountered in not-allowed context.",
                fieldRefSupported == FLE2FieldRefExpr::allowed);

        // The result of this prefix search will be either true or false, never encrypted. So
        // if the Subtree above us is comparing to an encrypted value that has to be an
        // error.
        ensureNotEncrypted("a fle $encStrStartsWith", subtreeStack);
        // Now that we're sure our result won't be compared to encrypted values, enter a new
        // Subtree to provide a new context for our children - this is a fresh start.
        Subtree::Compared encStrStartsWithSubtree;

        // $encStrStartsWith is of the form {$encStrStartsWith:{input: ‘$fieldname’, prefix: <target
        // string> }}. Obtain the field path here.
        auto fieldPathExpression =
            dynamic_cast<ExpressionFieldPath*>(encStrStartsWith->getChildren()[0].get());

        uassert(10112204,
                "$encStrStartsWith input must be an encrypted field.",
                isEncryptedFieldPath(fieldPathExpression));

        encStrStartsWithSubtree.temporarilyPermittedEncryptedFieldPath = fieldPathExpression;
        encStrStartsWithSubtree.encryptionPlaceholderContext =
            EncryptionPlaceholderContext::kTextPrefixComparison;

        enterSubtree(encStrStartsWithSubtree, subtreeStack);
    }
};

/**
 * This IntentionPostVisitor only replaces literals with placeholders for text search predicates.
 */
class IntentionPostVisitor final : public IntentionPostVisitorBase {
public:
    IntentionPostVisitor(const ExpressionContext& expCtx,
                         const EncryptionSchemaTreeNode& schema,
                         std::stack<Subtree>& subtreeStack)
        : IntentionPostVisitorBase(expCtx, schema, subtreeStack) {}

protected:
    using mongo::aggregate_expression_intender::IntentionPostVisitorBase::visit;

    void visit(ExpressionCompare* expr) override {
        switch (expr->getOp()) {
            case ExpressionCompare::EQ:
            case ExpressionCompare::NE:
            case ExpressionCompare::GT:
            case ExpressionCompare::GTE:
            case ExpressionCompare::LT:
            case ExpressionCompare::LTE: {
                // We use exitSubtreeNoReplacement() here to avoid generating placeholders for paths
                // that other walkers (range, equality) will create placeholders for.
                exitSubtreeNoReplacement<Subtree::Compared>(expCtx, subtreeStack);
                return;
            }
            case ExpressionCompare::CMP:
                didSetIntention =
                    exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
                return;
        }
    }

    void visit(ExpressionIn* in) override {
        if (dynamic_cast<ExpressionArray*>(in->getOperandList()[1].get())) {
            // We use exitSubtreeNoReplacement() here to avoid generating placeholders for paths
            // that other walkers (range, equality) will create.
            exitSubtreeNoReplacement<Subtree::Compared>(expCtx, subtreeStack);
        } else {
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
        }
    }

    void visit(ExpressionEncStrStartsWith*) override {
        didSetIntention = exitSubtree<Subtree::Compared>(expCtx, subtreeStack) || didSetIntention;
    }
};

class TextIntentionWalker final : public AggExprEncryptionIntentionWalkerBase {
public:
    TextIntentionWalker(ExpressionContext* expCtx,
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
    IntentionPostVisitor intentionPostVisitor{*expCtx, schema, subtreeStack};
};

}  // namespace

Intention markTextSearch(ExpressionContext* expCtx,
                         const EncryptionSchemaTreeNode& schema,
                         boost::intrusive_ptr<Expression>& expression,
                         bool expressionOutputIsCompared,
                         FLE2FieldRefExpr fieldRefSupported) {
    TextIntentionWalker walker{expCtx, schema, expressionOutputIsCompared, fieldRefSupported};
    expression_walker::walk<Expression>(expression.get(), &walker);
    return walker.exitOutermostSubtree(expressionOutputIsCompared);
}

}  // namespace mongo::aggregate_expression_intender
