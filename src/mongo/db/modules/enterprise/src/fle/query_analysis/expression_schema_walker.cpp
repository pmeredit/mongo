/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

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
    explicit SchemaTracker(bool outputIsCompared, FleVersion schemaVersion)
        : schemaVersion(schemaVersion), _outputIsCompared(outputIsCompared){};

    /**
     * Exits the current evaluation state. If exitting the outermost evaluated expression, then this
     * method will also populate the output schema as not encrypted under the assumption that the
     * result of the expression is being returned to the caller.
     */
    void exitEvaluateOrCompare() {
        invariant(_evaluateSubtreeCount > 0);
        --_evaluateSubtreeCount;
        if (_evaluateSubtreeCount == 0)
            reconcileSchema(std::make_unique<EncryptionSchemaNotEncryptedNode>(schemaVersion));
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
                    _outputSchema =
                        std::make_unique<EncryptionSchemaStateMixedNode>(newSchema->parsedFrom);
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
            reconcileSchema(std::make_unique<EncryptionSchemaNotEncryptedNode>(schemaVersion));
    }

    void decrementEvaluate() {
        invariant(_evaluateSubtreeCount > 0);
        --_evaluateSubtreeCount;
    }

    std::unique_ptr<EncryptionSchemaTreeNode> releaseOutputSchema() {
        return _outputSchema ? std::move(_outputSchema)
                             : std::make_unique<EncryptionSchemaNotEncryptedNode>(schemaVersion);
    }

    const FleVersion schemaVersion;

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
class ExpressionSchemaPreVisitor : public ExpressionConstVisitor {
public:
    ExpressionSchemaPreVisitor(const EncryptionSchemaTreeNode& schema, SchemaTracker& schemaTracker)
        : _schema(schema), _tracker(schemaTracker) {}

    void visit(const ExpressionAbs*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionAdd*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionAllElementsTrue*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionAnd*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionAnyElementTrue*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionArray*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionArrayElemAt*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionBitAnd*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionBitOr*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionBitXor*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionBitNot*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFirst*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionLast*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionObjectToArray*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionArrayToObject*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionBsonSize*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionCeil*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionCoerceToBool*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionCompare*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionConcat*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionConcatArrays*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateAdd*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateDiff*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateFromString*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateFromParts*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateSubtract*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateToParts*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateToString*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateTrunc*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDivide*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionExp*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFilter*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFloor*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFunction*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIn*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIndexOfArray*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIndexOfBytes*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIndexOfCP*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalJsEmit*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindElemMatch*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindPositional*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindSlice*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIsNumber*) {
        _tracker.enterEvaluateOrCompare();
    }

    void visit(const ExpressionLet*) {
        // aggregate_expression_intender::mark() handles disallowing rebinding CURRENT.
    }
    void visit(const ExpressionLn*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionLog*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionLog10*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFLEEqual*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFLEBetween*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionMap*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionMeta*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionMod*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionMultiply*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionNot*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionOr*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionPow*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionRange*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionReduce*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionReplaceOne*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionReplaceAll*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSetDifference*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSetEquals*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSetIntersection*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSetIsSubset*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSetUnion*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSize*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionReverseArray*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSortArray*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSlice*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIsArray*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindAllValuesAtPath*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionRound*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSplit*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSqrt*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionStrcasecmp*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSubstrBytes*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSubstrCP*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionStrLenBytes*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionBinarySize*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionStrLenCP*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSubtract*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionTestApiVersion*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionToLower*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionToUpper*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionTrim*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionTrunc*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionType*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionZip*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionConvert*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionRegexFind*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionRegexFindAll*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionRegexMatch*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionCosine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionTangent*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionArcCosine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionArcSine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionArcTangent*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionArcTangent2*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicArcTangent*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicArcCosine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicArcSine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicTangent*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicCosine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicSine*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDegreesToRadians*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionRadiansToDegrees*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDayOfMonth*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDayOfWeek*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDayOfYear*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionHour*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionMillisecond*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionMinute*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionMonth*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSecond*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionWeek*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIsoWeekYear*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIsoDayOfWeek*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIsoWeek*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionYear*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorAvg>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorFirstN>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorLastN>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorMax>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorMin>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorMaxN>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorMinN>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorQuantile<AccumulatorMedian>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorQuantile<AccumulatorPercentile>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorStdDevPop>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorStdDevSamp>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorSum>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorMergeObjects>*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionTests::Testable*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionRandom*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionToHashedIndexKey*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionGetField*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSetField*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionTsSecond*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionTsIncrement*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalOwningShard*) {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalIndexKey*) {
        _tracker.enterEvaluateOrCompare();
    }


    void visit(const ExpressionCond*) {
        // We enter evaluate on the first child (if branch), since the result of the expression is
        // always coerced to bool and not returned to the caller. The exit occurs between visiting
        // the 'if' and 'then' children in the InVisitor.
        _tracker.enterEvaluateOrCompare();
    }

    void visit(const ExpressionSwitch*) {
        _tracker.enterEvaluateOrCompare();
    }

    void visit(const ExpressionFieldPath* expr) {
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
            _tracker.reconcileSchema(
                std::make_unique<EncryptionSchemaNotEncryptedNode>(_tracker.schemaVersion));
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
                _tracker.reconcileSchema(
                    std::make_unique<EncryptionSchemaNotEncryptedNode>(_tracker.schemaVersion));
            }
        }
    }

    void visit(const ExpressionConstant*) {
        _tracker.reconcileLiteral();
    }

    void visit(const ExpressionIfNull*) {
        // Do not enter an evaluation subtree for $ifNull since either of the two children could be
        // returned depending on the evaluated result of the first.
    }

    void visit(const ExpressionObject* expr) {
        auto newSchema = std::make_unique<EncryptionSchemaNotEncryptedNode>(_tracker.schemaVersion);
        for (const auto& [field, childExpr] : expr->getChildExpressions()) {
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
};  // namespace

/**
 * Visitor which is called on the 'inVisit' for a given expression.
 */
class ExpressionSchemaInVisitor : public ExpressionConstVisitor {
public:
    ExpressionSchemaInVisitor(const EncryptionSchemaTreeNode& schema, SchemaTracker& schemaTracker)
        : _schema(schema), _tracker(schemaTracker) {}

    void visit(const ExpressionAbs*) {}
    void visit(const ExpressionAdd*) {}
    void visit(const ExpressionAllElementsTrue*) {}
    void visit(const ExpressionAnd*) {}
    void visit(const ExpressionAnyElementTrue*) {}
    void visit(const ExpressionArray*) {}
    void visit(const ExpressionArrayElemAt*) {}
    void visit(const ExpressionBitAnd*) {}
    void visit(const ExpressionBitOr*) {}
    void visit(const ExpressionBitXor*) {}
    void visit(const ExpressionBitNot*) {}
    void visit(const ExpressionFirst*) {}
    void visit(const ExpressionLast*) {}
    void visit(const ExpressionObjectToArray*) {}
    void visit(const ExpressionArrayToObject*) {}
    void visit(const ExpressionBsonSize*) {}
    void visit(const ExpressionCeil*) {}
    void visit(const ExpressionCoerceToBool*) {}
    void visit(const ExpressionCompare*) {}
    void visit(const ExpressionConcat*) {}
    void visit(const ExpressionConcatArrays*) {}
    void visit(const ExpressionConstant*) {}
    void visit(const ExpressionDateAdd*) {}
    void visit(const ExpressionDateDiff*) {}
    void visit(const ExpressionDateFromString*) {}
    void visit(const ExpressionDateFromParts*) {}
    void visit(const ExpressionDateSubtract*) {}
    void visit(const ExpressionDateToParts*) {}
    void visit(const ExpressionDateToString*) {}
    void visit(const ExpressionDateTrunc*) {}
    void visit(const ExpressionDivide*) {}
    void visit(const ExpressionExp*) {}
    void visit(const ExpressionFilter*) {}
    void visit(const ExpressionFloor*) {}
    void visit(const ExpressionFunction*) {}
    void visit(const ExpressionIfNull*) {}
    void visit(const ExpressionIn*) {}
    void visit(const ExpressionIndexOfArray*) {}
    void visit(const ExpressionIndexOfBytes*) {}
    void visit(const ExpressionIndexOfCP*) {}
    void visit(const ExpressionInternalJsEmit*) {}
    void visit(const ExpressionInternalFindElemMatch*) {}
    void visit(const ExpressionInternalFindPositional*) {}
    void visit(const ExpressionInternalFindSlice*) {}
    void visit(const ExpressionIsNumber*) {}
    void visit(const ExpressionLet*) {}
    void visit(const ExpressionLn*) {}
    void visit(const ExpressionLog*) {}
    void visit(const ExpressionLog10*) {}
    void visit(const ExpressionInternalFLEEqual*) {}
    void visit(const ExpressionInternalFLEBetween*) {}
    void visit(const ExpressionMap*) {}
    void visit(const ExpressionMeta*) {}
    void visit(const ExpressionMod*) {}
    void visit(const ExpressionMultiply*) {}
    void visit(const ExpressionNot*) {}
    void visit(const ExpressionObject*) {}
    void visit(const ExpressionOr*) {}
    void visit(const ExpressionPow*) {}
    void visit(const ExpressionRange*) {}
    void visit(const ExpressionReduce*) {}
    void visit(const ExpressionReplaceOne*) {}
    void visit(const ExpressionReplaceAll*) {}
    void visit(const ExpressionSetDifference*) {}
    void visit(const ExpressionSetEquals*) {}
    void visit(const ExpressionSetIntersection*) {}
    void visit(const ExpressionSetIsSubset*) {}
    void visit(const ExpressionSetUnion*) {}
    void visit(const ExpressionSize*) {}
    void visit(const ExpressionReverseArray*) {}
    void visit(const ExpressionSortArray*) {}
    void visit(const ExpressionSlice*) {}
    void visit(const ExpressionIsArray*) {}
    void visit(const ExpressionInternalFindAllValuesAtPath*) {}
    void visit(const ExpressionRound*) {}
    void visit(const ExpressionSplit*) {}
    void visit(const ExpressionSqrt*) {}
    void visit(const ExpressionStrcasecmp*) {}
    void visit(const ExpressionSubstrBytes*) {}
    void visit(const ExpressionSubstrCP*) {}
    void visit(const ExpressionStrLenBytes*) {}
    void visit(const ExpressionBinarySize*) {}
    void visit(const ExpressionStrLenCP*) {}
    void visit(const ExpressionSubtract*) {}
    void visit(const ExpressionTestApiVersion*) {}
    void visit(const ExpressionToLower*) {}
    void visit(const ExpressionToUpper*) {}
    void visit(const ExpressionTrim*) {}
    void visit(const ExpressionTrunc*) {}
    void visit(const ExpressionType*) {}
    void visit(const ExpressionZip*) {}
    void visit(const ExpressionConvert*) {}
    void visit(const ExpressionRegexFind*) {}
    void visit(const ExpressionRegexFindAll*) {}
    void visit(const ExpressionRegexMatch*) {}
    void visit(const ExpressionCosine*) {}
    void visit(const ExpressionSine*) {}
    void visit(const ExpressionTangent*) {}
    void visit(const ExpressionArcCosine*) {}
    void visit(const ExpressionArcSine*) {}
    void visit(const ExpressionArcTangent*) {}
    void visit(const ExpressionArcTangent2*) {}
    void visit(const ExpressionHyperbolicArcTangent*) {}
    void visit(const ExpressionHyperbolicArcCosine*) {}
    void visit(const ExpressionHyperbolicArcSine*) {}
    void visit(const ExpressionHyperbolicTangent*) {}
    void visit(const ExpressionHyperbolicCosine*) {}
    void visit(const ExpressionHyperbolicSine*) {}
    void visit(const ExpressionDegreesToRadians*) {}
    void visit(const ExpressionRadiansToDegrees*) {}
    void visit(const ExpressionDayOfMonth*) {}
    void visit(const ExpressionDayOfWeek*) {}
    void visit(const ExpressionDayOfYear*) {}
    void visit(const ExpressionHour*) {}
    void visit(const ExpressionMillisecond*) {}
    void visit(const ExpressionMinute*) {}
    void visit(const ExpressionMonth*) {}
    void visit(const ExpressionSecond*) {}
    void visit(const ExpressionWeek*) {}
    void visit(const ExpressionIsoWeekYear*) {}
    void visit(const ExpressionIsoDayOfWeek*) {}
    void visit(const ExpressionIsoWeek*) {}
    void visit(const ExpressionYear*) {}
    void visit(const ExpressionFromAccumulator<AccumulatorAvg>*) {}
    void visit(const ExpressionFromAccumulatorN<AccumulatorFirstN>*) {}
    void visit(const ExpressionFromAccumulatorN<AccumulatorLastN>*) {}
    void visit(const ExpressionFromAccumulator<AccumulatorMax>*) {}
    void visit(const ExpressionFromAccumulator<AccumulatorMin>*) {}
    void visit(const ExpressionFromAccumulatorN<AccumulatorMaxN>*) {}
    void visit(const ExpressionFromAccumulatorN<AccumulatorMinN>*) {}
    void visit(const ExpressionFromAccumulatorQuantile<AccumulatorMedian>*) {}
    void visit(const ExpressionFromAccumulatorQuantile<AccumulatorPercentile>*) {}
    void visit(const ExpressionFromAccumulator<AccumulatorStdDevPop>*) {}
    void visit(const ExpressionFromAccumulator<AccumulatorStdDevSamp>*) {}
    void visit(const ExpressionFromAccumulator<AccumulatorSum>*) {}
    void visit(const ExpressionFromAccumulator<AccumulatorMergeObjects>*) {}
    void visit(const ExpressionTests::Testable*) {}
    void visit(const ExpressionFieldPath*) {}
    void visit(const ExpressionRandom*) {}
    void visit(const ExpressionToHashedIndexKey*) {}
    void visit(const ExpressionGetField*) {}
    void visit(const ExpressionSetField*) {}
    void visit(const ExpressionTsSecond*) {}
    void visit(const ExpressionTsIncrement*) {}
    void visit(const ExpressionInternalOwningShard*) {}
    void visit(const ExpressionInternalIndexKey*) {}


    void visit(const ExpressionCond*) {
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

    void visit(const ExpressionSwitch* switchExpr) {
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
class ExpressionSchemaPostVisitor : public ExpressionConstVisitor {
public:
    ExpressionSchemaPostVisitor(const EncryptionSchemaTreeNode& schema,
                                SchemaTracker& schemaTracker)
        : _schema(schema), _tracker(schemaTracker) {}

    void visit(const ExpressionAbs*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionAdd*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionAllElementsTrue*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionAnd*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionAnyElementTrue*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionArray*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionArrayElemAt*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionBitAnd*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionBitOr*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionBitXor*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionBitNot*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFirst*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionLast*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionObjectToArray*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionArrayToObject*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionBsonSize*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionCeil*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionCoerceToBool*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionCompare*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionConcat*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionConcatArrays*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateAdd*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateDiff*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateFromString*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateFromParts*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateSubtract*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateToParts*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateToString*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateTrunc*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDivide*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionExp*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFilter*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFloor*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFunction*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIn*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIndexOfArray*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIndexOfBytes*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIndexOfCP*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalJsEmit*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindElemMatch*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindPositional*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindSlice*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIsNumber*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionLet*) {}
    void visit(const ExpressionLn*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionLog*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionLog10*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFLEEqual*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFLEBetween*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionMap*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionMeta*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionMod*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionMultiply*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionNot*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionOr*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionPow*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionRange*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionReduce*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionReplaceOne*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionReplaceAll*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSetDifference*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSetEquals*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSetIntersection*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSetIsSubset*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSetUnion*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSize*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionReverseArray*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSortArray*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSlice*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIsArray*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindAllValuesAtPath*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionRound*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSplit*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSqrt*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionStrcasecmp*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSubstrBytes*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSubstrCP*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionStrLenBytes*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionBinarySize*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionStrLenCP*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSubtract*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionTestApiVersion*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionToLower*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionToUpper*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionTrim*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionTrunc*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionType*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionZip*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionConvert*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionRegexFind*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionRegexFindAll*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionRegexMatch*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionCosine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionTangent*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionArcCosine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionArcSine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionArcTangent*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionArcTangent2*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicArcTangent*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicArcCosine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicArcSine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicTangent*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicCosine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicSine*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDegreesToRadians*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionRadiansToDegrees*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDayOfMonth*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDayOfWeek*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDayOfYear*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionHour*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionMillisecond*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionMinute*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionMonth*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSecond*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionWeek*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIsoWeekYear*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIsoDayOfWeek*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIsoWeek*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionYear*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorAvg>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorFirstN>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorLastN>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorMax>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorMin>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorMaxN>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorMinN>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorQuantile<AccumulatorMedian>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorQuantile<AccumulatorPercentile>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorStdDevPop>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorStdDevSamp>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorSum>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorMergeObjects>*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionTests::Testable*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionRandom*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionToHashedIndexKey*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionGetField*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSetField*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionTsSecond*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionTsIncrement*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalOwningShard*) {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalIndexKey*) {
        _tracker.exitEvaluateOrCompare();
    }


    // These expressions are not considered evaluated, however the interesting logic is handling in
    // the {Pre/In}Visitors.
    void visit(const ExpressionFieldPath*) {}
    void visit(const ExpressionConstant*) {}
    void visit(const ExpressionCond*) {}
    void visit(const ExpressionSwitch*) {}
    void visit(const ExpressionIfNull*) {}
    void visit(const ExpressionObject*) {
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
          _tracker(expressionOutputIsCompared, schema.parsedFrom),
          _preVisitor(_schema, _tracker),
          _inVisitor(_schema, _tracker),
          _postVisitor(_schema, _tracker) {}

    void preVisit(const Expression* expr) {
        expr->acceptVisitor(&_preVisitor);
    }
    void inVisit(unsigned long long count, const Expression* expr) {
        _inVisitor.numChildrenVisited = count;
        expr->acceptVisitor(&_inVisitor);
    }
    void postVisit(const Expression* expr) {
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
    expression_walker::walk<const Expression>(expression, &schemaWalker);
    return schemaWalker.releaseOutputSchema();
}

}  // namespace mongo::aggregate_expression_intender
