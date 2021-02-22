/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <boost/intrusive_ptr.hpp>

#include "aggregate_expression_intender.h"
#include "mongo/db/pipeline/expression_walker.h"

namespace mongo::aggregate_expression_intender {

namespace {

/**
 * Class responsible for storing the evaluation level of an expression being walked as well as the
 * output schema for the expression.
 */
class SchemaTracker {
public:
    explicit SchemaTracker(bool outputIsCompared) : _outputIsCompared(outputIsCompared){};

    /**
     * Exits the current evaluation state. If exitting the outermost evaluated expression, then this
     * method will also populate the output schema as not encrypted under the assumption that the
     * result of the expression is being returned to the caller.
     */
    void exitEvaluateOrCompare() {
        invariant(_evaluateSubtreeCount > 0);
        --_evaluateSubtreeCount;
        if (_evaluateSubtreeCount == 0)
            reconcileSchema(std::make_unique<EncryptionSchemaNotEncryptedNode>());
    }

    /**
     * Entering an "evaluation" expression means that the result is always not-encrypted. However,
     * we still keep track of the current nesting level to make sure that we only set the output
     * schema when exiting the outermost evaluated or compared expression.
     */
    void enterEvaluateOrCompare() {
        ++_evaluateSubtreeCount;
    }

    /**
     * Attempts to reconcile the 'newSchema' with the current output schema stored in _outputSchema.
     *
     * If called within an evaluated or compared expression (_evaluateSubtreeCount > 0), then this
     * method does nothing.
     */
    void reconcileSchema(std::unique_ptr<EncryptionSchemaTreeNode> newSchema) {
        if (_evaluateSubtreeCount == 0) {
            // If attempting to reconcile against a conflicting output schema, then instead return
            // an "unknown" node since the encryption schema is not known until runtime.
            if (_outputSchema) {
                // If the current reconciled schema is already unknown, then keep as-is since
                // comparing against it will result in an assertion.
                if (typeid(*_outputSchema) != typeid(EncryptionSchemaStateMixedNode) &&
                    *_outputSchema != *newSchema) {
                    _outputSchema = std::make_unique<EncryptionSchemaStateMixedNode>();
                }
            } else
                _outputSchema = std::move(newSchema);
        }
    }

    /**
     * Reconciling a literal is typically treated as unencrypted, unless the expression is being
     * used in a comparison between documents (e.g. $group key). In that case, do not attempt to
     * reconcile the schema as it may be compared to an encrypted field.
     */
    void reconcileLiteral() {
        if (!_outputIsCompared)
            reconcileSchema(std::make_unique<EncryptionSchemaNotEncryptedNode>());
    }

    void decrementEvaluate() {
        invariant(_evaluateSubtreeCount > 0);
        --_evaluateSubtreeCount;
    }

    std::unique_ptr<EncryptionSchemaTreeNode> releaseOutputSchema() {
        return _outputSchema ? std::move(_outputSchema)
                             : std::make_unique<EncryptionSchemaNotEncryptedNode>();
    }

private:
    // Tracks the evaluated nesting level of the current expression being visited. In general,
    // there's no work to be done within an evaluated expression (e.g. $add) since the result will
    // always be non-encrypted. However, we must track the depth of the evaluation to know when
    // we've exited or called 'postVisit' on the root of the evaluation sub-tree.
    unsigned long long _evaluateSubtreeCount{0};

    std::unique_ptr<EncryptionSchemaTreeNode> _outputSchema;

    // Indicates whether the output of the expression will be used in comparison between documents,
    // for instance as the key in a $group.
    bool _outputIsCompared;
};

/**
 * Visitor which is called on the 'preVisit' for a given expression.
 */
class ExpressionSchemaPreVisitor : public ExpressionVisitor {
public:
    ExpressionSchemaPreVisitor(const EncryptionSchemaTreeNode& schema, SchemaTracker& schemaTracker)
        : _schema(schema), _tracker(schemaTracker) {}

    void visit(ExpressionAbs*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionAdd*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionAllElementsTrue*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionAnd*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionAnyElementTrue*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionArray*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionArrayElemAt*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionFirst*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionLast*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionObjectToArray*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionArrayToObject*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionBsonSize*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionCeil*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionCoerceToBool*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionCompare*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionConcat*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionConcatArrays*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionDateAdd*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionDateDiff*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionDateFromString*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionDateFromParts*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionDateSubtract*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionDateToParts*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionDateToString*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionDateTrunc*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionDivide*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionExp*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionFilter*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionFloor*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionFunction*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionIn*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionIndexOfArray*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionIndexOfBytes*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionIndexOfCP*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionInternalJsEmit*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionInternalFindElemMatch*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionInternalFindPositional*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionInternalFindSlice*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionIsNumber*) {
        _tracker.enterEvaluateOrCompare();
    }

    void visit(ExpressionLet*) {
        // aggregate_expression_intender::mark() handles disallowing rebinding CURRENT.
    }
    void visit(ExpressionLn*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionLog*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionLog10*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionMap*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionMeta*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionMod*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionMultiply*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionNot*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionOr*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionPow*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionRange*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionReduce*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionReplaceOne*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionReplaceAll*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionSetDifference*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionSetEquals*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionSetIntersection*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionSetIsSubset*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionSetUnion*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionSize*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionReverseArray*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionSlice*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionIsArray*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionRound*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionSplit*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionSqrt*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionStrcasecmp*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionSubstrBytes*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionSubstrCP*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionStrLenBytes*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionBinarySize*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionStrLenCP*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionSubtract*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionTestApiVersion*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionToLower*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionToUpper*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionTrim*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionTrunc*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionType*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionZip*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionConvert*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionRegexFind*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionRegexFindAll*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionRegexMatch*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionCosine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionSine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionTangent*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionArcCosine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionArcSine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionArcTangent*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionArcTangent2*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionHyperbolicArcTangent*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionHyperbolicArcCosine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionHyperbolicArcSine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionHyperbolicTangent*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionHyperbolicCosine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionHyperbolicSine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionDegreesToRadians*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionRadiansToDegrees*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionDayOfMonth*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionDayOfWeek*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionDayOfYear*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionHour*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionMillisecond*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionMinute*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionMonth*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionSecond*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionWeek*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionIsoWeekYear*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionIsoDayOfWeek*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionIsoWeek*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionYear*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionFromAccumulator<AccumulatorAvg>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionFromAccumulator<AccumulatorMax>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionFromAccumulator<AccumulatorMin>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionFromAccumulator<AccumulatorSum>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionTests::Testable*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionRandom*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(ExpressionToHashedIndexKey*) {
        _tracker.enterEvaluateOrCompare();
    }

    void visit(ExpressionCond*) {
        // We enter evaluate on the first child (if branch), since the result of the expression is
        // always coerced to bool and not returned to the caller. The exit occurs between visiting
        // the 'if' and 'then' children in the InVisitor.
        _tracker.enterEvaluateOrCompare();
    }

    void visit(ExpressionSwitch*) {
        _tracker.enterEvaluateOrCompare();
    }

    void visit(ExpressionFieldPath* expr) {
        // The FieldPath is a variable.
        if (const auto& prefixedPath = expr->getFieldPath();
            prefixedPath.getFieldName(0) != "CURRENT" || prefixedPath.getPathLength() <= 1) {
            // Forbid CURRENT and ROOT. They could be supported after support for Object
            // comparisons is added.
            uassert(31127,
                    str::stream() << "Access to variable " + prefixedPath.getFieldName(0) +
                            " disallowed",
                    prefixedPath.getFieldName(0) != "CURRENT" &&
                        prefixedPath.getFieldName(0) != "ROOT");
            _tracker.reconcileSchema(std::make_unique<EncryptionSchemaNotEncryptedNode>());
            // The FieldPath is a field reference.
        } else {

            FieldRef path{expr->getFieldPathWithoutCurrentPrefix().fullPath()};

            // TODO SERVER-41337 Support field paths which are prefixes of encrypted fields.
            uassert(31129,
                    "Referencing a prefix of an encrypted field is not supported",
                    _schema.getEncryptionMetadataForPath(path) ||
                        !_schema.mayContainEncryptedNodeBelowPrefix(path));
            if (auto node = _schema.getNode(path)) {
                _tracker.reconcileSchema(node->clone());
            } else {
                _tracker.reconcileSchema(std::make_unique<EncryptionSchemaNotEncryptedNode>());
            }
        }
    }

    void visit(ExpressionConstant*) {
        _tracker.reconcileLiteral();
    }

    void visit(ExpressionIfNull*) {
        // Do not enter an evaluation subtree for $ifNull since either of the two children could be
        // returned depending on the evaluated result of the first.
    }

    void visit(ExpressionObject* expr) {
        auto newSchema = std::make_unique<EncryptionSchemaNotEncryptedNode>();
        for (auto [field, childExpr] : expr->getChildExpressions()) {
            newSchema->addChild(FieldRef(field), getOutputSchema(_schema, childExpr.get(), false));
        }
        _tracker.reconcileSchema(std::move(newSchema));

        // We enter an evaluate subtree here to avoid reconciling the schemas of each of the
        // object's fields.
        _tracker.enterEvaluateOrCompare();
    }

private:
    void throwNotSupported(std::string expr) {
        uasserted(31128, str::stream() << expr << " not supported with client-side encryption");
    }

    const EncryptionSchemaTreeNode& _schema;
    SchemaTracker& _tracker;
};

/**
 * Visitor which is called on the 'inVisit' for a given expression.
 */
class ExpressionSchemaInVisitor : public ExpressionVisitor {
public:
    ExpressionSchemaInVisitor(const EncryptionSchemaTreeNode& schema, SchemaTracker& schemaTracker)
        : _schema(schema), _tracker(schemaTracker) {}

    void visit(ExpressionAbs*) {}
    void visit(ExpressionAdd*) {}
    void visit(ExpressionAllElementsTrue*) {}
    void visit(ExpressionAnd*) {}
    void visit(ExpressionAnyElementTrue*) {}
    void visit(ExpressionArray*) {}
    void visit(ExpressionArrayElemAt*) {}
    void visit(ExpressionFirst*) {}
    void visit(ExpressionLast*) {}
    void visit(ExpressionObjectToArray*) {}
    void visit(ExpressionArrayToObject*) {}
    void visit(ExpressionBsonSize*) {}
    void visit(ExpressionCeil*) {}
    void visit(ExpressionCoerceToBool*) {}
    void visit(ExpressionCompare*) {}
    void visit(ExpressionConcat*) {}
    void visit(ExpressionConcatArrays*) {}
    void visit(ExpressionConstant*) {}
    void visit(ExpressionDateAdd*) {}
    void visit(ExpressionDateDiff*) {}
    void visit(ExpressionDateFromString*) {}
    void visit(ExpressionDateFromParts*) {}
    void visit(ExpressionDateSubtract*) {}
    void visit(ExpressionDateToParts*) {}
    void visit(ExpressionDateToString*) {}
    void visit(ExpressionDateTrunc*) {}
    void visit(ExpressionDivide*) {}
    void visit(ExpressionExp*) {}
    void visit(ExpressionFilter*) {}
    void visit(ExpressionFloor*) {}
    void visit(ExpressionFunction*) {}
    void visit(ExpressionIfNull*) {}
    void visit(ExpressionIn*) {}
    void visit(ExpressionIndexOfArray*) {}
    void visit(ExpressionIndexOfBytes*) {}
    void visit(ExpressionIndexOfCP*) {}
    void visit(ExpressionInternalJsEmit*) {}
    void visit(ExpressionInternalFindElemMatch*) {}
    void visit(ExpressionInternalFindPositional*) {}
    void visit(ExpressionInternalFindSlice*) {}
    void visit(ExpressionIsNumber*) {}
    void visit(ExpressionLet*) {}
    void visit(ExpressionLn*) {}
    void visit(ExpressionLog*) {}
    void visit(ExpressionLog10*) {}
    void visit(ExpressionMap*) {}
    void visit(ExpressionMeta*) {}
    void visit(ExpressionMod*) {}
    void visit(ExpressionMultiply*) {}
    void visit(ExpressionNot*) {}
    void visit(ExpressionObject*) {}
    void visit(ExpressionOr*) {}
    void visit(ExpressionPow*) {}
    void visit(ExpressionRange*) {}
    void visit(ExpressionReduce*) {}
    void visit(ExpressionReplaceOne*) {}
    void visit(ExpressionReplaceAll*) {}
    void visit(ExpressionSetDifference*) {}
    void visit(ExpressionSetEquals*) {}
    void visit(ExpressionSetIntersection*) {}
    void visit(ExpressionSetIsSubset*) {}
    void visit(ExpressionSetUnion*) {}
    void visit(ExpressionSize*) {}
    void visit(ExpressionReverseArray*) {}
    void visit(ExpressionSlice*) {}
    void visit(ExpressionIsArray*) {}
    void visit(ExpressionRound*) {}
    void visit(ExpressionSplit*) {}
    void visit(ExpressionSqrt*) {}
    void visit(ExpressionStrcasecmp*) {}
    void visit(ExpressionSubstrBytes*) {}
    void visit(ExpressionSubstrCP*) {}
    void visit(ExpressionStrLenBytes*) {}
    void visit(ExpressionBinarySize*) {}
    void visit(ExpressionStrLenCP*) {}
    void visit(ExpressionSubtract*) {}
    void visit(ExpressionTestApiVersion*) {}
    void visit(ExpressionToLower*) {}
    void visit(ExpressionToUpper*) {}
    void visit(ExpressionTrim*) {}
    void visit(ExpressionTrunc*) {}
    void visit(ExpressionType*) {}
    void visit(ExpressionZip*) {}
    void visit(ExpressionConvert*) {}
    void visit(ExpressionRegexFind*) {}
    void visit(ExpressionRegexFindAll*) {}
    void visit(ExpressionRegexMatch*) {}
    void visit(ExpressionCosine*) {}
    void visit(ExpressionSine*) {}
    void visit(ExpressionTangent*) {}
    void visit(ExpressionArcCosine*) {}
    void visit(ExpressionArcSine*) {}
    void visit(ExpressionArcTangent*) {}
    void visit(ExpressionArcTangent2*) {}
    void visit(ExpressionHyperbolicArcTangent*) {}
    void visit(ExpressionHyperbolicArcCosine*) {}
    void visit(ExpressionHyperbolicArcSine*) {}
    void visit(ExpressionHyperbolicTangent*) {}
    void visit(ExpressionHyperbolicCosine*) {}
    void visit(ExpressionHyperbolicSine*) {}
    void visit(ExpressionDegreesToRadians*) {}
    void visit(ExpressionRadiansToDegrees*) {}
    void visit(ExpressionDayOfMonth*) {}
    void visit(ExpressionDayOfWeek*) {}
    void visit(ExpressionDayOfYear*) {}
    void visit(ExpressionHour*) {}
    void visit(ExpressionMillisecond*) {}
    void visit(ExpressionMinute*) {}
    void visit(ExpressionMonth*) {}
    void visit(ExpressionSecond*) {}
    void visit(ExpressionWeek*) {}
    void visit(ExpressionIsoWeekYear*) {}
    void visit(ExpressionIsoDayOfWeek*) {}
    void visit(ExpressionIsoWeek*) {}
    void visit(ExpressionYear*) {}
    void visit(ExpressionFromAccumulator<AccumulatorAvg>*) {}
    void visit(ExpressionFromAccumulator<AccumulatorMax>*) {}
    void visit(ExpressionFromAccumulator<AccumulatorMin>*) {}
    void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>*) {}
    void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>*) {}
    void visit(ExpressionFromAccumulator<AccumulatorSum>*) {}
    void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>*) {}
    void visit(ExpressionTests::Testable*) {}
    void visit(ExpressionFieldPath*) {}
    void visit(ExpressionRandom*) {}
    void visit(ExpressionToHashedIndexKey*) {}


    void visit(ExpressionCond*) {
        // If the visited children count is 1, then this implies we've already visited the 'if'
        // child (0) and are about to visit the 'then' child (1). We should currently be in an
        // evaluated subtree due to the 'if' expression, and should exit this subtree before
        // visiting the 'then' and 'else' branches.
        if (numChildrenVisited == 1) {
            // Manually decrement the evaluated subtree count as we don't want to set the output
            // schema if the $cond is at the root.
            _tracker.decrementEvaluate();
        }
    }

    void visit(ExpressionSwitch* switchExpr) {
        // If the number of children visited is odd, then we're about to visit a 'then' expression
        // and need to consider the output schema. The exception is for the 'default' case, where
        // the number of children visited will be even.
        if (numChildrenVisited != switchExpr->getChildren().size() - 1) {
            if (numChildrenVisited % 2ull == 0ull)
                _tracker.enterEvaluateOrCompare();
            else
                // Manually decrement the evaluated subtree count as the evaluated result of the
                // 'case' expressions do not affect the output schema.
                _tracker.decrementEvaluate();
        } else {
            // Default case, consider the output schema of the expression.
        }
    }

    // Used to indicate which child expression is "in visit", since expressions hold their children
    // with an index into a vector.
    unsigned long long numChildrenVisited = 0;

private:
    const EncryptionSchemaTreeNode& _schema;
    SchemaTracker& _tracker;
};

/**
 * Visitor which is called on the 'postVisit' for a given expression.
 */
class ExpressionSchemaPostVisitor : public ExpressionVisitor {
public:
    ExpressionSchemaPostVisitor(const EncryptionSchemaTreeNode& schema,
                                SchemaTracker& schemaTracker)
        : _schema(schema), _tracker(schemaTracker) {}

    void visit(ExpressionAbs*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionAdd*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionAllElementsTrue*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionAnd*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionAnyElementTrue*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionArray*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionArrayElemAt*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionFirst*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionLast*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionObjectToArray*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionArrayToObject*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionBsonSize*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionCeil*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionCoerceToBool*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionCompare*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionConcat*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionConcatArrays*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionDateAdd*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionDateDiff*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionDateFromString*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionDateFromParts*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionDateSubtract*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionDateToParts*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionDateToString*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionDateTrunc*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionDivide*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionExp*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionFilter*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionFloor*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionFunction*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionIn*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionIndexOfArray*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionIndexOfBytes*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionIndexOfCP*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionInternalJsEmit*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionInternalFindElemMatch*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionInternalFindPositional*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionInternalFindSlice*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionIsNumber*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionLet*) {}
    void visit(ExpressionLn*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionLog*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionLog10*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionMap*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionMeta*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionMod*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionMultiply*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionNot*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionOr*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionPow*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionRange*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionReduce*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionReplaceOne*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionReplaceAll*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionSetDifference*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionSetEquals*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionSetIntersection*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionSetIsSubset*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionSetUnion*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionSize*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionReverseArray*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionSlice*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionIsArray*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionRound*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionSplit*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionSqrt*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionStrcasecmp*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionSubstrBytes*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionSubstrCP*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionStrLenBytes*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionBinarySize*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionStrLenCP*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionSubtract*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionTestApiVersion*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionToLower*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionToUpper*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionTrim*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionTrunc*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionType*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionZip*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionConvert*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionRegexFind*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionRegexFindAll*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionRegexMatch*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionCosine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionSine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionTangent*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionArcCosine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionArcSine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionArcTangent*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionArcTangent2*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionHyperbolicArcTangent*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionHyperbolicArcCosine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionHyperbolicArcSine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionHyperbolicTangent*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionHyperbolicCosine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionHyperbolicSine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionDegreesToRadians*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionRadiansToDegrees*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionDayOfMonth*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionDayOfWeek*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionDayOfYear*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionHour*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionMillisecond*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionMinute*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionMonth*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionSecond*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionWeek*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionIsoWeekYear*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionIsoDayOfWeek*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionIsoWeek*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionYear*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionFromAccumulator<AccumulatorAvg>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionFromAccumulator<AccumulatorMax>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionFromAccumulator<AccumulatorMin>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionFromAccumulator<AccumulatorSum>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionTests::Testable*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionRandom*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(ExpressionToHashedIndexKey*) {
        _tracker.exitEvaluateOrCompare();
    }

    // These expressions are not considered evaluated, however the interesting logic is handling in
    // the {Pre/In}Visitors.
    void visit(ExpressionFieldPath*) {}
    void visit(ExpressionConstant*) {}
    void visit(ExpressionCond*) {}
    void visit(ExpressionSwitch*) {}
    void visit(ExpressionIfNull*) {}
    void visit(ExpressionObject*) {
        // Manually call 'decrementEvaluate' instead of 'exitEvaluateOrCompare' to avoid the
        // implicit assumption that an evaluated expression results in a not encrypted schema.
        _tracker.decrementEvaluate();
    }

private:
    const EncryptionSchemaTreeNode& _schema;
    SchemaTracker& _tracker;
};

/**
 * ExpressionWalker which takes an input schema and returns a new schema representing the result of
 * evaluating the expression being walked.
 */
class ExpressionWalkerSchema {
public:
    ExpressionWalkerSchema(const EncryptionSchemaTreeNode& schema, bool expressionOutputIsCompared)
        : _schema(schema),
          _tracker(expressionOutputIsCompared),
          _preVisitor(_schema, _tracker),
          _inVisitor(_schema, _tracker),
          _postVisitor(_schema, _tracker) {}

    void preVisit(Expression* expr) {
        expr->acceptVisitor(&_preVisitor);
    }
    void inVisit(unsigned long long count, Expression* expr) {
        _inVisitor.numChildrenVisited = count;
        expr->acceptVisitor(&_inVisitor);
    }
    void postVisit(Expression* expr) {
        expr->acceptVisitor(&_postVisitor);
    }

    std::unique_ptr<EncryptionSchemaTreeNode> releaseOutputSchema() {
        return _tracker.releaseOutputSchema();
    }

private:
    const EncryptionSchemaTreeNode& _schema;

    // Each visitor holds a reference to a 'SchemaTracker', which tracks the evaluated subtree level
    // as well as the output schema.
    SchemaTracker _tracker;

    ExpressionSchemaPreVisitor _preVisitor;
    ExpressionSchemaInVisitor _inVisitor;
    ExpressionSchemaPostVisitor _postVisitor;
};

}  // namespace

std::unique_ptr<EncryptionSchemaTreeNode> getOutputSchema(const EncryptionSchemaTreeNode& schema,
                                                          Expression* expression,
                                                          bool expressionOutputIsCompared) {
    ExpressionWalkerSchema schemaWalker{schema, expressionOutputIsCompared};
    expression_walker::walk(&schemaWalker, expression);
    return schemaWalker.releaseOutputSchema();
}

}  // namespace mongo::aggregate_expression_intender
