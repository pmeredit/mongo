/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <fmt/format.h>
#include <functional>
#include <numeric>
#include <stack>
#include <string>
#include <type_traits>
#include <utility>

#include "aggregate_expression_intender_range.h"
#include "mongo/base/string_data.h"
#include "mongo/db/pipeline/expression_find_internal.h"
#include "mongo/db/pipeline/expression_function.h"
#include "mongo/db/pipeline/expression_js_emit.h"
#include "mongo/db/pipeline/expression_test_api_version.h"
#include "mongo/db/pipeline/expression_trigonometric.h"
#include "mongo/db/pipeline/expression_visitor.h"
#include "mongo/db/pipeline/expression_walker.h"
#include "mongo/db/pipeline/window_function/window_function_expression.h"
#include "mongo/stdx/variant.h"
#include "query_analysis.h"
#include "resolved_encryption_info.h"

namespace mongo::aggregate_expression_intender {

namespace {

using namespace fmt::literals;
using namespace std::string_literals;
/**
 * Struct used to move expressions between visitors to allow for proper replacement.
 */
struct VisitorSharedState {
    boost::intrusive_ptr<Expression> newEncryptedExpression = nullptr;

    /**
     * Returns Intention::Marked if a replacement occurred. 'operandNumber' is expected to be
     * 1-indexed as the visitors keep track of "how many children have been visited" as
     * opposed to "which operand we just visited".
     */
    Intention replaceOperandWithEncryptedExpressionIfPresent(Expression* expr, int operandNumber) {
        if (!newEncryptedExpression) {
            return Intention::NotMarked;
        }
        std::vector<boost::intrusive_ptr<Expression>>& children = expr->getChildren();
        // operandNumber is how many children have been visited. Adjust to be zero indexed.
        tassert(6721402, "Expected 1-indexed operandNumber", operandNumber != 0);
        children[operandNumber - 1].swap(newEncryptedExpression);
        newEncryptedExpression = nullptr;

        return Intention::Marked;
    }
};

/**
 * Helper to determine if a comparison expression has exactly a field path and a constant. Returns
 * a pointer to each of the children of that type, or nullptrs if the types are not correct.
 */
std::pair<ExpressionFieldPath*, ExpressionConstant*> getFieldPathAndConstantFromComparison(
    ExpressionCompare* compare) {

    auto firstChild = compare->getOperandList()[0].get();
    auto firstOpFieldPath = dynamic_cast<ExpressionFieldPath*>(firstChild);
    auto secondChild = compare->getOperandList()[1].get();
    auto secondOpFieldPath = dynamic_cast<ExpressionFieldPath*>(secondChild);
    if (firstOpFieldPath) {
        return {firstOpFieldPath, dynamic_cast<ExpressionConstant*>(secondChild)};
    } else if (secondOpFieldPath) {
        return {secondOpFieldPath, dynamic_cast<ExpressionConstant*>(firstChild)};
    }
    return {nullptr, nullptr};
}
/**
 * For a range index we generate the new expressions that need to replace comparison operators
 * but defer the actual replacement to the In/Post visitors.
 */
class IntentionPreVisitor final : public IntentionPreVisitorBase {
public:
    IntentionPreVisitor(ExpressionContext* expCtx,
                        const EncryptionSchemaTreeNode& schema,
                        std::stack<Subtree>& subtreeStack,
                        FLE2FieldRefExpr fieldRefSupported,
                        VisitorSharedState* sharedState)
        : mongo::aggregate_expression_intender::IntentionPreVisitorBase(
              expCtx, schema, subtreeStack, fieldRefSupported),
          _sharedState(sharedState) {}

protected:
    using mongo::aggregate_expression_intender::IntentionPreVisitorBase::visit;
    virtual void visit(ExpressionCompare* compare) override final {
        // The result of this comparison will be either true or false, never encrypted. So
        // if the Subtree above us is comparing to an encrypted value that has to be an
        // error.
        ensureNotEncrypted("a comparison", subtreeStack);

        // The below work needs to be done for all cases except $cmp, which can never
        // be encrypted. Enter an Eval subtree immediately instead of descending into
        // a comparison that allows encryption.
        if (compare->getOp() == ExpressionCompare::CMP) {
            ensureNotEncryptedEnterEval("a three-way comparison", subtreeStack);
            return;
        }
        // Now that we're sure our result won't be compared to encrypted values, enter a new
        // Subtree to provide a new context for our children - this is a fresh start.
        Subtree::Compared comparedSubtree;
        auto isEncryptedFieldPath = [&](ExpressionFieldPath* fieldPathExpr) {
            if (fieldPathExpr) {
                auto ref = fieldPathExpr->getFieldPathWithoutCurrentPrefix().fullPath();
                return schema.getEncryptionMetadataForPath(FieldRef{ref}) ||
                    schema.mayContainEncryptedNodeBelowPrefix(FieldRef{ref});
            }
            return false;
        };

        // For any ExpressionCompare (except $cmp) we need to identify whether it is exactly
        // an encrypted field path and a constant. If it is, we need to replace it. Otherwise
        // if there is an encrypted field we error.
        bool includesEquals = false;
        auto [relevantPath, relevantConstant] = getFieldPathAndConstantFromComparison(compare);
        if (!isEncryptedFieldPath(relevantPath)) {
            // Enter the compared subtree but don't allow any encryption.
            enterSubtree(comparedSubtree, subtreeStack);
            return;
        }
        ensureFLE2EncryptedFieldComparedToConstant(relevantPath, relevantConstant);

        // The tree traversal will continue after this node, make sure now that we've
        // validated encryption is allowed future nodes don't error.
        comparedSubtree.temporarilyPermittedEncryptedFieldPath = relevantPath;
        auto path = relevantPath->getFieldPath();
        // It isn't possible to change $$CURRENT for query_analysis, so it is safe to remove. All
        // paths are relative to the root.
        if (path.front() == "CURRENT") {
            path = path.tail();
        }
        auto metadata = schema.getEncryptionMetadataForPath(FieldRef(path.fullPath()));
        tassert(
            6721405, str::stream() << "Expected metadata for path " << path.fullPath(), metadata);
        // Expect exactly one index on this field.
        uassert(6721409,
                "Need ranged index to issue an encrypted ranged query",
                metadata->fle2SupportedQueries && metadata->fle2SupportedQueries->size() == 1);
        // Get relevant information for building a placeholder encrypted between.
        auto indexInfo = metadata->fle2SupportedQueries.get()[0];
        auto ki = metadata->keyId.uuids()[0];

        auto min = BSON("" << -std::numeric_limits<double>::infinity());
        auto max = BSON("" << std::numeric_limits<double>::infinity());
        // We need to build a BSONElement to pass to $encryptedBetween later.
        auto wrappedConst = relevantConstant->getValue().wrap("");
        switch (compare->getOp()) {
            case ExpressionCompare::EQ: {
                auto encryptedBetweenExpr = buildExpressionEncryptedBetweenWithPlaceholder(
                    expCtx,
                    path.fullPath(),
                    ki,
                    indexInfo,
                    {wrappedConst.firstElement(), true},
                    {wrappedConst.firstElement(), true});
                _sharedState->newEncryptedExpression = std::move(encryptedBetweenExpr);
                enterSubtree(comparedSubtree, subtreeStack);
                return;
            }
            case ExpressionCompare::NE: {
                // We can only make range placeholders, so represent != X as NOT (X, X)
                auto encryptedBetweenExpr = buildExpressionEncryptedBetweenWithPlaceholder(
                    expCtx,
                    path.fullPath(),
                    ki,
                    indexInfo,
                    {wrappedConst.firstElement(), true},
                    {wrappedConst.firstElement(), true});
                std::vector<boost::intrusive_ptr<Expression>> arg = {encryptedBetweenExpr};
                boost::intrusive_ptr<ExpressionNot> notExpr =
                    make_intrusive<ExpressionNot>(expCtx, std::move(arg));
                _sharedState->newEncryptedExpression = std::move(notExpr);
                enterSubtree(comparedSubtree, subtreeStack);
                return;
            }
            case ExpressionCompare::GTE:
                includesEquals = true;
                [[fallthrough]];  // The only difference between this and the following is equals.
            case ExpressionCompare::GT: {
                auto encryptedBetween = buildExpressionEncryptedBetweenWithPlaceholder(
                    expCtx,
                    path.fullPath(),
                    ki,
                    indexInfo,
                    {wrappedConst.firstElement(), includesEquals},
                    {max.firstElement(), true});
                _sharedState->newEncryptedExpression = std::move(encryptedBetween);
                enterSubtree(comparedSubtree, subtreeStack);
                return;
            }
            case ExpressionCompare::LTE:
                includesEquals = true;
                [[fallthrough]];  // The only difference between this and the following is equals.
            case ExpressionCompare::LT: {
                auto encryptedBetween = buildExpressionEncryptedBetweenWithPlaceholder(
                    expCtx,
                    path.fullPath(),
                    ki,
                    indexInfo,
                    {min.firstElement(), true},
                    {wrappedConst.firstElement(), includesEquals});
                _sharedState->newEncryptedExpression = std::move(encryptedBetween);
                enterSubtree(comparedSubtree, subtreeStack);
                return;
            }
            case ExpressionCompare::CMP:
                // We addressed this above.
                MONGO_UNREACHABLE_TASSERT(6721408);
        }
    }

    /**
     * No work to do. We've already generated a replacement expression if necessary.
     */
    virtual void visit(ExpressionConstant* expr) override {}

private:
    FLE2FieldRefExpr fieldRefSupported;
    VisitorSharedState* _sharedState = nullptr;
};

class IntentionInVisitor final : public IntentionInVisitorBase {
    using mongo::aggregate_expression_intender::IntentionInVisitorBase::visit;

public:
    IntentionInVisitor(const ExpressionContext& expCtx,
                       const EncryptionSchemaTreeNode& schema,
                       std::stack<Subtree>& subtreeStack,
                       VisitorSharedState* sharedState)
        : IntentionInVisitorBase(expCtx, schema, subtreeStack), _sharedState(sharedState) {}

protected:
    virtual void visit(ExpressionAbs* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionAdd* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionAllElementsTrue* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionAnd* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionAnyElementTrue* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionArray* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionArrayElemAt* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFirst* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionLast* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionObjectToArray* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionArrayToObject* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionBsonSize* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionCeil* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionCoerceToBool* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionCompare* expr) override {
        switch (expr->getOp()) {
            case ExpressionCompare::EQ:
            case ExpressionCompare::NE:
            case ExpressionCompare::GT:
            case ExpressionCompare::GTE:
            case ExpressionCompare::LT:
            case ExpressionCompare::LTE: {
                auto childExprs = getFieldPathAndConstantFromComparison(expr);
                // If one of our children is not of the expected type, we did not generate the
                // replacement expression.
                if (!childExprs.first || !childExprs.second) {
                    internalPerformReplacement(expr);
                }
                return;
            }
            case ExpressionCompare::CMP:
                internalPerformReplacement(expr);
                return;
        }
    }
    virtual void visit(ExpressionConcat* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionConcatArrays* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionCond* expr) override {
        internalPerformReplacement(expr);
        IntentionInVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionDateAdd* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionDateDiff* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionDateFromString* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionDateFromParts* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionDateSubtract* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionDateToParts* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionDateToString* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionDateTrunc* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionDivide* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionBetween* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionExp* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFieldPath* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFilter* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFloor* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFunction* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionGetField* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSetField* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionTestApiVersion* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionToHashedIndexKey* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionIfNull* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionIn* in) override {
        uasserted(6721414, "ExpressionIn with RangeIndex not yet supported");
        internalPerformReplacement(in);
        IntentionInVisitor::visit(in);
    }
    virtual void visit(ExpressionIndexOfArray* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionIndexOfBytes* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionIndexOfCP* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionInternalJsEmit* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionInternalFindElemMatch* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionInternalFindPositional* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionInternalFindSlice* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionIsNumber* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionLn* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionLog* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionLog10* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionInternalFLEEqual* expr) override {
        MONGO_UNREACHABLE_TASSERT(6721410);
    }
    virtual void visit(ExpressionInternalFLEBetween* expr) override {
        MONGO_UNREACHABLE_TASSERT(6721411);
    }
    virtual void visit(ExpressionMap* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionMeta* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionMod* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionMultiply* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionNot* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionObject* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionOr* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionPow* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionRandom* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionRange* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionReduce* reduce) override {
        internalPerformReplacement(reduce);
        IntentionInVisitorBase::visit(reduce);
    }
    virtual void visit(ExpressionReplaceOne* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionReplaceAll* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSetDifference* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSetEquals* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSetIntersection* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSetIsSubset* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSetUnion* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSize* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionReverseArray* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSortArray* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSlice* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionIsArray* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionInternalFindAllValuesAtPath* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionRound* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSplit* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSqrt* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionStrcasecmp* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSubstrBytes* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSubstrCP* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionStrLenBytes* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionBinarySize* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionStrLenCP* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSubtract* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSwitch* switchExpr) override {
        internalPerformReplacement(switchExpr);
        IntentionInVisitorBase::visit(switchExpr);
    }
    virtual void visit(ExpressionToLower* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionToUpper* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionTrim* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionTrunc* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionType* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionZip* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionConvert* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionRegexFind* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionRegexFindAll* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionRegexMatch* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionCosine* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSine* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionTangent* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionArcCosine* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionArcSine* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionArcTangent* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionArcTangent2* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionHyperbolicArcTangent* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionHyperbolicArcCosine* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionHyperbolicArcSine* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionHyperbolicTangent* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionHyperbolicCosine* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionHyperbolicSine* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionDegreesToRadians* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionRadiansToDegrees* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionDayOfMonth* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionDayOfWeek* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionDayOfYear* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionHour* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionMillisecond* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionMinute* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionMonth* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionSecond* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionWeek* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionIsoWeekYear* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionIsoDayOfWeek* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionIsoWeek* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionYear* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorAvg>* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorFirstN>* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorLastN>* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorMax>* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorMin>* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorMaxN>* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorMinN>* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorSum>* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionTsSecond* expr) override {
        internalPerformReplacement(expr);
    }
    virtual void visit(ExpressionTsIncrement* expr) override {
        internalPerformReplacement(expr);
    }

private:
    VisitorSharedState* _sharedState;
    void internalPerformReplacement(Expression* expr) {
        didSetIntention = _sharedState->replaceOperandWithEncryptedExpressionIfPresent(
                              expr, numChildrenVisited) ||
            didSetIntention;
    }
};

class IntentionPostVisitor final : public IntentionPostVisitorBase {
public:
    IntentionPostVisitor(const ExpressionContext& expCtx,
                         const EncryptionSchemaTreeNode& schema,
                         std::stack<Subtree>& subtreeStack,
                         VisitorSharedState* sharedState)
        : IntentionPostVisitorBase(expCtx, schema, subtreeStack), _sharedState(sharedState) {}

protected:
    using mongo::aggregate_expression_intender::IntentionPostVisitorBase::visit;

    virtual void visit(ExpressionAbs* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionAdd* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionAllElementsTrue* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionAnd* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionAnyElementTrue* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionArray* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionArrayElemAt* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFirst* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionLast* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionObjectToArray* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionArrayToObject* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionBsonSize* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionCeil* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionCoerceToBool* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionCompare* expr) override {
        switch (expr->getOp()) {
            case ExpressionCompare::EQ:
            case ExpressionCompare::NE:
            case ExpressionCompare::GT:
            case ExpressionCompare::GTE:
            case ExpressionCompare::LT:
            case ExpressionCompare::LTE: {
                auto childExprs = getFieldPathAndConstantFromComparison(expr);
                // If one of our children is not of the expected type, we did not generate the
                // replacement expression.
                if (!childExprs.first || !childExprs.second) {
                    internalPerformReplacement(expr);
                }
                didSetIntention =
                    exitSubtree<Subtree::Compared>(expCtx, subtreeStack) || didSetIntention;
                return;
            }
            case ExpressionCompare::CMP:
                internalPerformReplacement(expr);
                didSetIntention =
                    exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
                return;
        }
    }
    virtual void visit(ExpressionConcat* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionConcatArrays* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionCond* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionDateAdd* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionDateDiff* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionDateFromParts* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionDateFromString* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionDateSubtract* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionDateToParts* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionDateToString* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionDateTrunc* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionDivide* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionBetween* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionExp* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFieldPath* expr) override {
        // This will be performed by expressionCompare if necessary.
        // internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFilter* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFloor* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFunction* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionGetField* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSetField* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionTestApiVersion* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionToHashedIndexKey* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionIfNull* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionIn* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionIndexOfArray* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionIndexOfBytes* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionIsNumber* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionIndexOfCP* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionInternalJsEmit* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionInternalFindElemMatch* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionInternalFindPositional* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionInternalFindSlice* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionLet* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionLn* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionLog* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionLog10* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionInternalFLEEqual* expr) override {
        MONGO_UNREACHABLE_TASSERT(6721412);
    }
    virtual void visit(ExpressionInternalFLEBetween* expr) override {
        MONGO_UNREACHABLE_TASSERT(6721413);
    }
    virtual void visit(ExpressionMap* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionMeta* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionMod* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionMultiply* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionNot* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionObject* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionOr* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionPow* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionRandom* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionRange* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionReduce* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionReplaceAll* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSetDifference* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSetEquals* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSetIntersection* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSetIsSubset* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSetUnion* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSize* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionReverseArray* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSortArray* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSlice* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionIsArray* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionInternalFindAllValuesAtPath* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionRound* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSplit* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSqrt* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionStrcasecmp* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSubstrBytes* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSubstrCP* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionStrLenBytes* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionBinarySize* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionStrLenCP* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSubtract* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSwitch* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
        // required here.
    }
    virtual void visit(ExpressionToLower* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionToUpper* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionTrim* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionTrunc* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionType* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionZip* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionConvert* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionRegexFind* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionRegexFindAll* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionRegexMatch* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionCosine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionTangent* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionArcCosine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionArcSine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionArcTangent* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionArcTangent2* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionHyperbolicArcTangent* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionHyperbolicArcCosine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionHyperbolicArcSine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionHyperbolicTangent* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionHyperbolicCosine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionHyperbolicSine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionDegreesToRadians* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionRadiansToDegrees* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionDayOfMonth* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionDayOfWeek* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionDayOfYear* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionHour* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionMillisecond* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionMinute* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionMonth* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionSecond* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionWeek* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionIsoWeekYear* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionIsoDayOfWeek* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionIsoWeek* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionYear* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorAvg>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorFirstN>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorLastN>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorMax>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorMin>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorMaxN>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorMinN>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorSum>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionTsSecond* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    virtual void visit(ExpressionTsIncrement* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }

    void internalPerformReplacement(Expression* expr) {
        didSetIntention = _sharedState->replaceOperandWithEncryptedExpressionIfPresent(
                              expr, expr->getChildren().size()) ||
            didSetIntention;
    }
    VisitorSharedState* _sharedState = nullptr;
};  // namespace
class RangeIntentionWalker final : public AggExprEncryptionIntentionWalkerBase {
public:
    RangeIntentionWalker(ExpressionContext* expCtx,
                         const EncryptionSchemaTreeNode& schema,
                         bool expressionOutputIsCompared,
                         FLE2FieldRefExpr fieldRefSupported)
        : AggExprEncryptionIntentionWalkerBase(expCtx, schema, expressionOutputIsCompared),
          fieldRefSupported(fieldRefSupported) {}

    Intention exitOutermostSubtreeRange(bool expressionOutputIsCompared,
                                        boost::intrusive_ptr<Expression>& expr) {
        // When walking is complete, exit the outermost Subtree and report whether any fields were
        // marked in the execution of the walker.
        Intention rootSubtreeSetIntention = expressionOutputIsCompared
            ? exitSubtree<Subtree::Compared>(*expCtx, subtreeStack)
            : exitSubtree<Subtree::Forwarded>(*expCtx, subtreeStack);
        if (visitorSharedState.newEncryptedExpression) {
            expr.swap(visitorSharedState.newEncryptedExpression);
            visitorSharedState.newEncryptedExpression = nullptr;
            rootSubtreeSetIntention = Intention::Marked;
        }
        return rootSubtreeSetIntention || getPostVisitor()->didSetIntention ||
            getInVisitor()->didSetIntention;
    }

    Intention exitOutermostSubtree(bool expressionOutputIsCompared) override {
        // Use exitOutermostSubtreeRange instead.
        MONGO_UNREACHABLE_TASSERT(6721404);
    }


private:
    virtual IntentionPreVisitorBase* getPreVisitor() override final {
        return &intentionPreVisitor;
    }
    virtual IntentionInVisitorBase* getInVisitor() override final {
        return &intentionInVisitor;
    }
    virtual IntentionPostVisitorBase* getPostVisitor() override final {
        return &intentionPostVisitor;
    }

    // For now this should always be true for RangeWalkers, as we only generate encrypted
    // placeholders when we know they're allowed.
    FLE2FieldRefExpr fieldRefSupported;
    VisitorSharedState visitorSharedState;
    IntentionPreVisitor intentionPreVisitor{
        expCtx, schema, subtreeStack, fieldRefSupported, &visitorSharedState};
    IntentionInVisitor intentionInVisitor{*expCtx, schema, subtreeStack, &visitorSharedState};
    IntentionPostVisitor intentionPostVisitor{*expCtx, schema, subtreeStack, &visitorSharedState};
};

}  // namespace

Intention markRange(ExpressionContext* expCtx,
                    const EncryptionSchemaTreeNode& schema,
                    boost::intrusive_ptr<Expression>& expression,
                    bool expressionOutputIsCompared,
                    FLE2FieldRefExpr fieldRefSupported) {
    RangeIntentionWalker walker{expCtx, schema, expressionOutputIsCompared, fieldRefSupported};
    expression_walker::walk<Expression>(expression.get(), &walker);
    return walker.exitOutermostSubtreeRange(expressionOutputIsCompared, expression);
}

}  // namespace mongo::aggregate_expression_intender
