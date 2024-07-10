/**
 * Copyright (C) 2022-present MongoDB, Inc. and subject to applicable commercial license.
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
#include "mongo/db/pipeline/accumulator_percentile.h"
#include "mongo/db/pipeline/expression_find_internal.h"
#include "mongo/db/pipeline/expression_from_accumulator_quantile.h"
#include "mongo/db/pipeline/expression_function.h"
#include "mongo/db/pipeline/expression_js_emit.h"
#include "mongo/db/pipeline/expression_test_api_version.h"
#include "mongo/db/pipeline/expression_trigonometric.h"
#include "mongo/db/pipeline/expression_visitor.h"
#include "mongo/db/pipeline/expression_walker.h"
#include "mongo/db/pipeline/window_function/window_function_expression.h"
#include "mongo/db/query/interval.h"
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

FieldPath stripCurrentIfPresent(FieldPath path) {
    if (path.front() == "CURRENT") {
        return path.tail();
    }
    return path;
}

/**
 * Functions that take either a value or an ExpressionObject. These recursively walk the object and
 * report the full paths and values at all leaf nodes in the object. In the ExpressionObject version
 * this will error on any non-constant or non-object expressions, as these are not supported for
 * encrypted comparisons.
 */
std::vector<std::pair<FieldPath, Value>> reportFullPathsAndValues(ExpressionObject* objExpr,
                                                                  FieldPath basePath) {
    std::vector<std::pair<FieldPath, Value>> retVec;
    // This is a vector of string field path pairs to expressions.
    auto childExprs = objExpr->getChildExpressions();
    for (const auto& [fieldPathStr, childExpr] : childExprs) {
        std::vector<std::pair<FieldPath, Value>> thisChildPaths;
        if (auto constantExpr = dynamic_cast<ExpressionConstant*>(childExpr.get())) {
            auto constantValue = constantExpr->getValue();
            // Traverse constant object in array to find all encrypted fields.
            thisChildPaths = query_analysis::reportFullPathsAndValues(
                constantValue, basePath.concat(fieldPathStr));
        } else if (auto objectExpr = dynamic_cast<ExpressionObject*>(childExpr.get())) {
            thisChildPaths = reportFullPathsAndValues(objectExpr, basePath.concat(fieldPathStr));
        } else {
            uasserted(6994302, "Can only compare encrypted field to object or constant in $in");
        }
        retVec.insert(retVec.begin(), thisChildPaths.begin(), thisChildPaths.end());
    }
    return retVec;
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

    Intention didSetIntention = Intention::NotMarked;

protected:
    using mongo::aggregate_expression_intender::IntentionPreVisitorBase::visit;
    BSONObj kMinDoubleObj = BSON("" << -std::numeric_limits<double>::infinity());
    BSONObj kMaxDoubleObj = BSON("" << std::numeric_limits<double>::infinity());
    BSONObj kMaxDateObj = BSON("" << Date_t::max());
    BSONObj kMinDateObj = BSON("" << Date_t::min());

    /**
     * Returns the max for the given type if 'max' is true, otherwise returns min.
     */
    BSONElement getBoundForType(BSONType type, bool max) {
        switch (type) {
            case (BSONType::Date):
                return max ? kMaxDateObj.firstElement() : kMinDateObj.firstElement();
            case (BSONType::NumberInt):
            case (BSONType::NumberLong):
            case (BSONType::NumberDecimal):
            case (BSONType::NumberDouble):
                return max ? kMaxDoubleObj.firstElement() : kMinDoubleObj.firstElement();
            default:
                tasserted(7020505,
                          str::stream() << "Invalid type for getMaxForType " << typeName(type));
        }
        MONGO_UNREACHABLE_TASSERT(7020506);
    }
    /**
     * When doing comparisons with dates we want to use the proper date infinity values, but
     * encrypted indexes use double on either side. When building encrypted range placeholders,
     * replace the dates with doubles.
     */
    BSONElement replaceInfiniteDateBoundWithDoubleBound(BSONElement bound) {
        if (bound.type() != BSONType::Date)
            return bound;
        auto dateVal = bound.Date();
        if (dateVal == Date_t::min()) {
            return kMinDoubleObj.firstElement();
        } else if (dateVal == Date_t::max()) {
            return kMaxDoubleObj.firstElement();
        }
        return bound;
    }
    void visit(ExpressionCompare* compare) final {
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

        // For any ExpressionCompare (except $cmp) we need to identify whether it is exactly
        // an encrypted field path and a constant. If it is, we need to replace it. Otherwise
        // if there is an encrypted field we error.
        bool includesEquals = false;
        auto [relevantPath, relevantConstant] = getFieldPathAndConstantFromExpression(compare);
        if (!isEncryptedFieldPath(relevantPath)) {
            // Enter the compared subtree but don't allow any encryption.
            enterSubtree(comparedSubtree, subtreeStack);
            return;
        }
        ensureFLE2EncryptedFieldComparedToConstant(relevantPath, relevantConstant);

        auto path = relevantPath->getFieldPath();
        // It isn't possible to change $$CURRENT for query_analysis, so it is safe to remove. All
        // paths are relative to the root.
        path = stripCurrentIfPresent(path);
        auto metadata = schema.getEncryptionMetadataForPath(FieldRef(path.fullPath()));
        tassert(
            6721405, str::stream() << "Expected metadata for path " << path.fullPath(), metadata);
        comparedSubtree.temporarilyPermittedEncryptedFieldPath = relevantPath;
        auto constVal = relevantConstant->getValue();
        if (!metadata->algorithmIs(Fle2AlgorithmInt::kRange) || isEncryptedPayload(constVal)) {
            // We only replace range encrypted fields here OR we're walking a previously encrypted
            // path. Enter the compared subtree, and allow encryption at the path. The equality
            // walker may deal with this when it walks the tree, we don't want to error out.
            enterSubtree(comparedSubtree, subtreeStack);
            return;
        }
        // The tree traversal will continue after this node, make sure now that we've
        // validated encryption is allowed future nodes don't error.
        // Expect exactly one index on this field.
        uassert(6721409,
                "Need ranged index to issue an encrypted ranged query",
                metadata->fle2SupportedQueries && metadata->fle2SupportedQueries->size() == 1);

        // At this point we know we will need to do a replacement, make sure we're in an allowed
        // context.
        uassert(7020507,
                "Encrypted expression encountered in not-allowed context",
                fieldRefSupported == FLE2FieldRefExpr::allowed);
        // Get relevant information for building a placeholder encrypted between.
        auto indexInfo = metadata->fle2SupportedQueries.get()[0];
        auto ki = metadata->keyId.uuids()[0];

        auto wrappedConst = constVal.wrap("");
        uassert(6720810,
                "Constant for encrypted comparison must be in range bounds",
                literalWithinRangeBounds(*metadata, wrappedConst.firstElement()));
        // We're going to replace our children.
        compare->getChildren().clear();
        switch (compare->getOp()) {
            case ExpressionCompare::EQ: {
                auto encryptedRangeExpr =
                    buildEncryptedRangeWithPlaceholder(expCtx,
                                                       path.fullPath(),
                                                       ki,
                                                       indexInfo,
                                                       {wrappedConst.firstElement(), true},
                                                       {wrappedConst.firstElement(), true},
                                                       getRangePayloadId());
                _sharedState->newEncryptedExpression = std::move(encryptedRangeExpr);
                enterSubtree(comparedSubtree, subtreeStack);
                return;
            }
            case ExpressionCompare::NE: {
                // We can only make range placeholders, so represent != X as NOT (X, X)
                auto encryptedRangeExpr =
                    buildEncryptedRangeWithPlaceholder(expCtx,
                                                       path.fullPath(),
                                                       ki,
                                                       indexInfo,
                                                       {wrappedConst.firstElement(), true},
                                                       {wrappedConst.firstElement(), true},
                                                       getRangePayloadId());
                std::vector<boost::intrusive_ptr<Expression>> arg = {encryptedRangeExpr};
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
                auto encryptedRange = buildEncryptedRangeWithPlaceholder(
                    expCtx,
                    path.fullPath(),
                    ki,
                    indexInfo,
                    {wrappedConst.firstElement(), includesEquals},
                    {kMaxDoubleObj.firstElement(), true},
                    getRangePayloadId());  // Encrypted between always uses doubles.
                _sharedState->newEncryptedExpression = std::move(encryptedRange);
                enterSubtree(comparedSubtree, subtreeStack);
                return;
            }
            case ExpressionCompare::LTE:
                includesEquals = true;
                [[fallthrough]];  // The only difference between this and the following is equals.
            case ExpressionCompare::LT: {
                auto encryptedRange = buildEncryptedRangeWithPlaceholder(
                    expCtx,
                    path.fullPath(),
                    ki,
                    indexInfo,
                    {kMinDoubleObj.firstElement(), true},  // Encrypted between always uses doubles.
                    {wrappedConst.firstElement(), includesEquals},
                    getRangePayloadId());
                _sharedState->newEncryptedExpression = std::move(encryptedRange);
                enterSubtree(comparedSubtree, subtreeStack);
                return;
            }
            case ExpressionCompare::CMP:
                // We addressed this above.
                MONGO_UNREACHABLE_TASSERT(6721408);
        }
    }

    /**
     * An ExpressionAnd can perform optimizations if it knows it has multiple ExpressionCompares
     * on the same field.
     */
    void visit(ExpressionAnd* expr) override {
        // We will track the following:
        // 1. FieldPath of the ExpressionCompare.
        // 2. An interval list for that field.
        std::map<std::string, std::vector<Interval>> fpIntervalMap;
        // Iterate over the children. Use a while loop as we may modify the iterator/vector in the
        // middle.
        auto& childVec = expr->getChildren();
        auto childIt = childVec.begin();
        while (childIt != childVec.end()) {
            ExpressionCompare* compareExpr = dynamic_cast<ExpressionCompare*>(childIt->get());
            if (!compareExpr) {
                // Only ExpressionCompare instances will be replaced.
                ++childIt;
                continue;
            }
            auto [fp, constant] = getFieldPathAndConstantFromExpression(compareExpr);
            if (!constant || !isEncryptedFieldPath(fp)) {
                // This is the structure required to replace.
                ++childIt;
                continue;
            }
            auto comparisonPath = stripCurrentIfPresent(fp->getFieldPath()).fullPath();
            auto metadata = schema.getEncryptionMetadataForPath(FieldRef(comparisonPath));
            if (!metadata->algorithmIs(Fle2AlgorithmInt::kRange)) {
                // We only replace range encrypted fields here.
                ++childIt;
                continue;
            }
            // At this point we know we have an encrypted field, make sure we're in an allowed
            // context.
            uassert(7020508,
                    "Encrypted expression encountered in not-allowed context",
                    fieldRefSupported == mongo::FLE2FieldRefExpr::allowed);
            bool lbInclusive = false;
            bool ubInclusive = false;
            BSONObj intervalBase = BSONObj();
            auto constantValue = constant->getValue();
            if (isEncryptedPayload(constantValue)) {
                // This field path was encrypted earlier.
                ++childIt;
                continue;
            }
            uassert(6720802,
                    "Expected number constant",
                    constantValue.numeric() || constantValue.getType() == BSONType::Date);
            uassert(6720811,
                    "Constant for encrypted comparison must be in range bounds",
                    literalWithinRangeBounds(*metadata, constantValue.wrap("").firstElement()));
            if (!metadata) {
                // This path is not encrypted.
                ++childIt;
                continue;
            }
            switch (compareExpr->getOp()) {
                case ExpressionCompare::EQ:
                    // Pass. EQ doesn't need to be combined.
                case ExpressionCompare::NE:
                    // Pass. NE doesn't need to be combined.
                    ++childIt;
                    continue;
                case ExpressionCompare::GTE:
                    lbInclusive = true;
                    [[fallthrough]];  // The only difference between this and the following is
                                      // equals.
                case ExpressionCompare::GT: {
                    auto thisInterval =
                        Interval(BSON("min" << constantValue << "max"
                                            << getBoundForType(constantValue.getType(), true)),
                                 lbInclusive,
                                 true);
                    if (fpIntervalMap.count(comparisonPath) > 0) {
                        auto fpItr = fpIntervalMap.find(comparisonPath);
                        fpItr->second.push_back(thisInterval);
                    } else {
                        fpIntervalMap.insert({comparisonPath, {thisInterval}});
                    }
                    // We will recreate this expression later.
                    childIt = childVec.erase(childIt);
                    continue;
                }
                case ExpressionCompare::LTE:
                    ubInclusive = true;
                    [[fallthrough]];  // The only difference between this and the following is
                                      // equals.
                case ExpressionCompare::LT: {
                    auto thisInterval =
                        Interval(BSON("min" << getBoundForType(constantValue.getType(), false)
                                            << "max" << constantValue),
                                 true,
                                 ubInclusive);
                    if (fpIntervalMap.count(comparisonPath) > 0) {
                        auto fpItr = fpIntervalMap.find(comparisonPath);
                        fpItr->second.push_back(thisInterval);
                    } else {
                        fpIntervalMap.insert({comparisonPath, {thisInterval}});
                    }
                    // We will recreate this expression later.
                    childIt = childVec.erase(childIt);
                    continue;
                }
                case ExpressionCompare::CMP:
                    ++childIt;
                    continue;
            }
            MONGO_UNREACHABLE_TASSERT(6720801);
        }

        // We now have a list of intervals for each field. Attempt to combine them.
        std::vector<boost::intrusive_ptr<Expression>> newEncryptedChildren;
        // For each field path in the $and:
        for (auto& [childFP, startingIntervals] : fpIntervalMap) {
            std::vector<Interval> finalIntervals;
            finalIntervals.push_back(startingIntervals[0]);
            // For each GT/GTE/LT/LTE child we saw:
            bool containsEmptyInterval = false;
            for (size_t i = 1; i < startingIntervals.size(); ++i) {
                // See if we can combine it with a different option we saw.
                bool combined = false;
                for (unsigned long long j = 0; j < finalIntervals.size(); ++j) {
                    auto intervalCompare = startingIntervals[i].compare(finalIntervals[j]);
                    if (intervalCompare != Interval::IntervalComparison::INTERVAL_PRECEDES &&
                        intervalCompare != Interval::IntervalComparison::INTERVAL_SUCCEEDS) {
                        finalIntervals[j].combine(startingIntervals[i], intervalCompare);
                        combined = true;
                        // We can only combine this with one interval.
                        break;
                    } else {
                        // We've seen two intervals that together contain no values.
                        containsEmptyInterval = true;
                        break;
                    }
                }
                if (containsEmptyInterval) {
                    // No point in combining more intervals, we know this query can't match
                    // anything.
                    break;
                }
                if (!combined) {
                    finalIntervals.push_back(startingIntervals[i]);
                }
            }
            if (containsEmptyInterval) {
                // We know two of the children of this $and have conflicting predicates and will
                // always evaluate to false. Don't pass extra expressions to evaluate to the server.
                // Note that this optimization is not correct for arrays, but we know array fields
                // cannot be encrypted.
                newEncryptedChildren.clear();
                childVec.clear();
                childVec.insert(childVec.end(),
                                make_intrusive<ExpressionConstant>(expCtx, Value(false)));
                break;
            }
            auto metadata = schema.getEncryptionMetadataForPath(FieldRef(childFP));
            tassert(6720800, "Expected metadata for path", metadata);
            for (const auto& interval : finalIntervals) {
                newEncryptedChildren.push_back(buildEncryptedRangeWithPlaceholder(
                    expCtx,
                    childFP,
                    metadata->keyId.uuids()[0],
                    metadata->fle2SupportedQueries.get()[0],
                    {replaceInfiniteDateBoundWithDoubleBound(interval.start),
                     interval.startInclusive},
                    {replaceInfiniteDateBoundWithDoubleBound(interval.end), interval.endInclusive},
                    getRangePayloadId()));
            }
        }
        if (newEncryptedChildren.size() > 0) {
            childVec.insert(
                childVec.end(), newEncryptedChildren.begin(), newEncryptedChildren.end());
            // We've done replacements, record as such.
            didSetIntention = Intention::Marked;
        }
        // We've done all the necessary work for encrypted GT/GTE/LT/LTE nodes below this
        // expression, but we may do more work for the other children.
        IntentionPreVisitorBase::visit(expr);
    }

    void visit(ExpressionIn* in) override {
        // Regardless of the below analysis, an $in expression is going to output an unencrypted
        // boolean. So if the result of this expression is being compared to encrypted values, it's
        // not going to work.
        ensureNotEncrypted("an $in expression", subtreeStack);
        // In most cases we can't work with arrays in this visitor, but $in is an interesting
        // exception.
        //     If the second argument to $in is an array literal, we know that the things inside
        // that array are going to be compared to the first argument and so we can build
        // placeholders for those comparisons.
        //     If however the second argument is not an array literal then we must fail if it
        // contains anything encrypted. For example, if we have
        // {$in: ["xx-yyy-zzz", "$allowlistedSSNs"]} and 'allowlistedSSNs' is encrypted, we won't be
        // able to look within the array to evaluate the $in. So in these cases we add an
        // 'Evaluated' Subtree to make sure none of the arguments are encrypted.
        if (auto arrExpr = dynamic_cast<ExpressionArray*>(in->getOperandList()[1].get())) {
            // We also specifically support cases like: {$in: ["$encryptedField", <arr>]}. We build
            // placeholders here to support this case.
            auto comparedSubtree = Subtree::Compared{};
            if (auto firstOp = dynamic_cast<ExpressionFieldPath*>(in->getOperandList()[0].get())) {
                auto ref = firstOp->getFieldPathWithoutCurrentPrefix().fullPath();
                auto inReplacementOrExpr = make_intrusive<ExpressionOr>(expCtx);
                if (auto metadata = schema.getEncryptionMetadataForPath(FieldRef{ref});
                    metadata && metadata->algorithmIs(Fle2AlgorithmInt::kRange)) {
                    // At this point we know we will need to do a replacement, make sure we're in an
                    // allowed context.
                    uassert(6994304,
                            "Encrypted expression encountered in not-allowed context in $in",
                            fieldRefSupported == FLE2FieldRefExpr::allowed);
                    for (auto& elem : arrExpr->getChildren()) {
                        ensureFLE2EncryptedFieldComparedToConstant(firstOp, elem.get());
                        auto constantExpr = dynamic_cast<ExpressionConstant*>(elem.get());
                        auto constantValue = constantExpr->getValue();
                        literalWithinRangeBounds(metadata->fle2SupportedQueries.get()[0],
                                                 constantValue.wrap("").firstElement());
                        inReplacementOrExpr->addOperand(buildEncryptedRangeWithPlaceholder(
                            expCtx,
                            ref,
                            metadata->keyId.uuids()[0],
                            metadata->fle2SupportedQueries.get()[0],
                            {constantValue.wrap("").firstElement(), true},
                            {constantValue.wrap("").firstElement(), true},
                            getRangePayloadId()));
                    }
                    // We've generated a complete replacement for this $in. Remove the children as
                    // we don't need to traverse them.
                    in->getChildren().clear();
                    _sharedState->newEncryptedExpression = inReplacementOrExpr;
                } else if (schema.getEncryptionMetadataForPath(FieldRef{ref}) ||
                           !schema.mayContainEncryptedNodeBelowPrefix(FieldRef{ref})) {
                    // This path is encrypted with a different algorithm or not encrypted.
                    comparedSubtree.temporarilyPermittedEncryptedFieldPath = firstOp;
                } else if (schema.mayContainEncryptedNodeBelowPrefix(FieldRef{ref})) {
                    // TODO SERVER-71093 Support $in below encrypted prefix.
                    uassert(7036804,
                            "Cannot use $in with a fieldpath that is a prefix of a range encrypted "
                            "field",
                            !schema.mayContainRangeEncryptedNodeBelowPrefix(FieldRef{ref}));
                }
            }
            enterSubtree(comparedSubtree, subtreeStack);
        } else {
            enterSubtree(Subtree::Evaluated{"an $in comparison without an array literal"},
                         subtreeStack);
        }
    }

private:
    VisitorSharedState* _sharedState = nullptr;
    int32_t rangePredicateCounter = 0;

    int32_t getRangePayloadId() {
        return rangePredicateCounter++;
    }
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
    void visit(ExpressionAbs* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionAdd* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionAllElementsTrue* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionAnd* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionAnyElementTrue* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionArray* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionArrayElemAt* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFirst* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionLast* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionObjectToArray* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionArrayToObject* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionBsonSize* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionCeil* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionCoerceToBool* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionCompare* expr) override {
        switch (expr->getOp()) {
            case ExpressionCompare::EQ:
            case ExpressionCompare::NE:
            case ExpressionCompare::GT:
            case ExpressionCompare::GTE:
            case ExpressionCompare::LT:
            case ExpressionCompare::LTE: {
                // When an ExpressionCompare generates a replacement, it removes its children. If we
                // have children we must not have generated a replacement, so use the one our child
                // generated if present.
                if (expr->getChildren().size() != 0) {
                    internalPerformReplacement(expr);
                }
                return;
            }
            case ExpressionCompare::CMP:
                internalPerformReplacement(expr);
                return;
        }
    }
    void visit(ExpressionConcat* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionConcatArrays* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionCond* expr) override {
        internalPerformReplacement(expr);
        IntentionInVisitorBase::visit(expr);
    }
    void visit(ExpressionDateAdd* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionDateDiff* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionDateFromString* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionDateFromParts* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionDateSubtract* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionDateToParts* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionDateToString* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionDateTrunc* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionDivide* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionExp* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFieldPath* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFilter* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFloor* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFunction* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionGetField* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSetField* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionTestApiVersion* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionToHashedIndexKey* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionIfNull* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionIn* in) override {
        // $in may need to be replaced, but only supports constants. Therefore it never needs to do
        // replacement.
        uassert(
            6721414, "ExpressionIn cannot replace children", !_sharedState->newEncryptedExpression);
        IntentionInVisitorBase::visit(in);
    }
    void visit(ExpressionIndexOfArray* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionIndexOfBytes* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionIndexOfCP* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionInternalJsEmit* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionInternalFindElemMatch* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionInternalFindPositional* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionInternalFindSlice* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionIsNumber* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionLn* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionLog* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionLog10* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionInternalFLEEqual* expr) override {
        MONGO_UNREACHABLE_TASSERT(6721410);
    }
    void visit(ExpressionInternalFLEBetween* expr) override {
        MONGO_UNREACHABLE_TASSERT(6721411);
    }
    void visit(ExpressionMap* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionMeta* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionMod* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionMultiply* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionNot* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionObject* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionOr* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionPow* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionRandom* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionRange* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionReduce* reduce) override {
        internalPerformReplacement(reduce);
        IntentionInVisitorBase::visit(reduce);
    }
    void visit(ExpressionReplaceOne* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionReplaceAll* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSetDifference* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSetEquals* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSetIntersection* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSetIsSubset* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSetUnion* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSize* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionReverseArray* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSortArray* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSlice* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionIsArray* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionInternalFindAllValuesAtPath* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionRound* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSplit* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSqrt* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionStrcasecmp* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSubstrBytes* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSubstrCP* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionStrLenBytes* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionBinarySize* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionStrLenCP* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSubtract* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSwitch* switchExpr) override {
        internalPerformReplacement(switchExpr);
        IntentionInVisitorBase::visit(switchExpr);
    }
    void visit(ExpressionToLower* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionToUpper* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionTrim* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionTrunc* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionType* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionZip* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionConvert* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionRegexFind* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionRegexFindAll* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionRegexMatch* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionCosine* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSine* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionTangent* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionArcCosine* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionArcSine* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionArcTangent* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionArcTangent2* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionHyperbolicArcTangent* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionHyperbolicArcCosine* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionHyperbolicArcSine* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionHyperbolicTangent* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionHyperbolicCosine* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionHyperbolicSine* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionDegreesToRadians* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionRadiansToDegrees* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionDayOfMonth* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionDayOfWeek* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionDayOfYear* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionHour* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionMillisecond* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionMinute* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionMonth* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionSecond* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionWeek* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionIsoWeekYear* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionIsoDayOfWeek* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionIsoWeek* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionYear* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFromAccumulator<AccumulatorAvg>* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorFirstN>* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorLastN>* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFromAccumulator<AccumulatorMax>* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFromAccumulator<AccumulatorMin>* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorMaxN>* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorMinN>* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFromAccumulatorQuantile<AccumulatorMedian>* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFromAccumulatorQuantile<AccumulatorPercentile>* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFromAccumulator<AccumulatorSum>* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionTsSecond* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionTsIncrement* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionInternalOwningShard* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionInternalIndexKey* expr) override {
        internalPerformReplacement(expr);
    }
    void visit(ExpressionInternalKeyStringValue* expr) override {
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

    void visit(ExpressionAbs* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionAdd* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionAllElementsTrue* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionAnd* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionAnyElementTrue* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionArray* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionArrayElemAt* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFirst* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionLast* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionObjectToArray* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionArrayToObject* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionBsonSize* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionCeil* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionCoerceToBool* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionCompare* expr) override {
        switch (expr->getOp()) {
            case ExpressionCompare::EQ:
            case ExpressionCompare::NE:
            case ExpressionCompare::GT:
            case ExpressionCompare::GTE:
            case ExpressionCompare::LT:
            case ExpressionCompare::LTE: {
                // When an ExpressionCompare generates a replacement, it removes its children. If we
                // have children we must not have generated a replacement, so use the one our child
                // generated if present.
                if (expr->getChildren().size() != 0) {
                    internalPerformReplacement(expr);
                }
                exitSubtreeNoReplacement<Subtree::Compared>(expCtx, subtreeStack);
                return;
            }
            case ExpressionCompare::CMP:
                internalPerformReplacement(expr);
                exitSubtreeNoReplacement<Subtree::Evaluated>(expCtx, subtreeStack);
                return;
        }
    }
    void visit(ExpressionConcat* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionConcatArrays* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionCond* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionDateAdd* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionDateDiff* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionDateFromParts* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionDateFromString* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionDateSubtract* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionDateToParts* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionDateToString* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionDateTrunc* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionDivide* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionExp* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFieldPath* expr) override {
        // Replacement will be performed by expressionCompare if necessary.
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFilter* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFloor* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFunction* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionGetField* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSetField* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionTestApiVersion* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionToHashedIndexKey* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionIfNull* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionIn* expr) override {
        if (expr->getOperandList().size() == 0) {
            // If we cleared the array, we know we already did replacement.
            didSetIntention =
                exitSubtree<Subtree::Compared>(expCtx, subtreeStack) || didSetIntention;
            return;
        }
        // $in doesn't perform replacement. Exit whatever subtree we entered.
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionIndexOfArray* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionIndexOfBytes* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionIsNumber* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionIndexOfCP* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionInternalJsEmit* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionInternalFindElemMatch* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionInternalFindPositional* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionInternalFindSlice* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionLet* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionLn* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionLog* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionLog10* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionInternalFLEEqual* expr) override {
        MONGO_UNREACHABLE_TASSERT(6721412);
    }
    void visit(ExpressionInternalFLEBetween* expr) override {
        MONGO_UNREACHABLE_TASSERT(6721413);
    }
    void visit(ExpressionMap* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionMeta* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionMod* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionMultiply* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionNot* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionObject* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionOr* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionPow* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionRandom* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionRange* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionReduce* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionReplaceAll* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSetDifference* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSetEquals* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSetIntersection* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSetIsSubset* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSetUnion* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSize* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionReverseArray* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSortArray* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSlice* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionIsArray* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionInternalFindAllValuesAtPath* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionRound* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSplit* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSqrt* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionStrcasecmp* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSubstrBytes* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSubstrCP* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionStrLenBytes* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionBinarySize* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionStrLenCP* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSubtract* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSwitch* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
        // required here.
    }
    void visit(ExpressionToLower* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionToUpper* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionTrim* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionTrunc* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionType* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionZip* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionConvert* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionRegexFind* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionRegexFindAll* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionRegexMatch* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionCosine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionTangent* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionArcCosine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionArcSine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionArcTangent* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionArcTangent2* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionHyperbolicArcTangent* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionHyperbolicArcCosine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionHyperbolicArcSine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionHyperbolicTangent* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionHyperbolicCosine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionHyperbolicSine* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionDegreesToRadians* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionRadiansToDegrees* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionDayOfMonth* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionDayOfWeek* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionDayOfYear* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionHour* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionMillisecond* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionMinute* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionMonth* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionSecond* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionWeek* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionIsoWeekYear* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionIsoDayOfWeek* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionIsoWeek* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionYear* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFromAccumulator<AccumulatorAvg>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorFirstN>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorLastN>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFromAccumulator<AccumulatorMax>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFromAccumulator<AccumulatorMin>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorMaxN>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorMinN>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFromAccumulatorQuantile<AccumulatorMedian>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFromAccumulatorQuantile<AccumulatorPercentile>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFromAccumulator<AccumulatorSum>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionTsSecond* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionTsIncrement* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionInternalOwningShard* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionInternalIndexKey* expr) override {
        internalPerformReplacement(expr);
        IntentionPostVisitorBase::visit(expr);
    }
    void visit(ExpressionInternalKeyStringValue* expr) override {
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
        // When walking is complete, exit the outermost Subtree and report whether any fields
        // were marked in the execution of the walker.
        if (expressionOutputIsCompared) {
            exitSubtreeNoReplacement<Subtree::Compared>(*expCtx, subtreeStack);
        } else {
            exitSubtreeNoReplacement<Subtree::Forwarded>(*expCtx, subtreeStack);
        }
        Intention rootSubtreeSetIntention = Intention::NotMarked;
        if (visitorSharedState.newEncryptedExpression) {
            expr.swap(visitorSharedState.newEncryptedExpression);
            visitorSharedState.newEncryptedExpression = nullptr;
            rootSubtreeSetIntention = Intention::Marked;
        }
        return rootSubtreeSetIntention || getPostVisitor()->didSetIntention ||
            getInVisitor()->didSetIntention || intentionPreVisitor.didSetIntention;
    }

    Intention exitOutermostSubtree(bool expressionOutputIsCompared) override {
        // Use exitOutermostSubtreeRange instead.
        MONGO_UNREACHABLE_TASSERT(6721404);
    }


private:
    IntentionPreVisitorBase* getPreVisitor() final {
        return &intentionPreVisitor;
    }
    IntentionInVisitorBase* getInVisitor() final {
        return &intentionInVisitor;
    }
    IntentionPostVisitorBase* getPostVisitor() final {
        return &intentionPostVisitor;
    }

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
