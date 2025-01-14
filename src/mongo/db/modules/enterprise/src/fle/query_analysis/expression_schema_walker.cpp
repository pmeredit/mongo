/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <boost/intrusive_ptr.hpp>

#include "aggregate_expression_intender.h"
#include "mongo/db/pipeline/expression_from_accumulator_quantile.h"
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

    void visit(const ExpressionAbs*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionAdd*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionAllElementsTrue*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionAnd*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionAnyElementTrue*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionArray*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionArrayElemAt*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionBitAnd*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionBitOr*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionBitXor*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionBitNot*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFirst*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionLast*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionObjectToArray*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionArrayToObject*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionBsonSize*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionCeil*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionCoerceToBool*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionCompare*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionConcat*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionConcatArrays*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateAdd*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateDiff*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateFromString*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateFromParts*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateSubtract*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateToParts*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateToString*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDateTrunc*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDivide*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionExp*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFilter*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFloor*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFunction*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIn*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIndexOfArray*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIndexOfBytes*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIndexOfCP*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalJsEmit*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindElemMatch*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindPositional*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindSlice*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIsNumber*) override {
        _tracker.enterEvaluateOrCompare();
    }

    void visit(const ExpressionLet*) override {
        // aggregate_expression_intender::mark() handles disallowing rebinding CURRENT.
    }
    void visit(const ExpressionLn*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionLog*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionLog10*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFLEEqual*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFLEBetween*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalRawSortKey*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionMap*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionMeta*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionMod*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionMultiply*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionNot*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionOr*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionPow*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionRange*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionReduce*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionReplaceOne*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionReplaceAll*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSetDifference*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSetEquals*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSetIntersection*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSetIsSubset*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSetUnion*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSize*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionReverseArray*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSortArray*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSlice*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIsArray*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindAllValuesAtPath*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionRound*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSplit*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSqrt*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionStrcasecmp*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSubstrBytes*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSubstrCP*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionStrLenBytes*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionBinarySize*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionStrLenCP*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSubtract*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionTestApiVersion*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionToLower*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionToUpper*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionTrim*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionTrunc*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionType*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionZip*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionConvert*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionRegexFind*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionRegexFindAll*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionRegexMatch*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionCosine*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSine*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionTangent*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionArcCosine*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionArcSine*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionArcTangent*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionArcTangent2*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicArcTangent*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicArcCosine*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicArcSine*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicTangent*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicCosine*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicSine*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDegreesToRadians*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionRadiansToDegrees*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDayOfMonth*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDayOfWeek*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionDayOfYear*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionHour*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionMillisecond*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionMinute*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionMonth*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSecond*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionWeek*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIsoWeekYear*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIsoDayOfWeek*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionIsoWeek*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionYear*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorAvg>*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorFirstN>*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorLastN>*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorMax>*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorMin>*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorMaxN>*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorMinN>*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorQuantile<AccumulatorMedian>*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorQuantile<AccumulatorPercentile>*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorStdDevPop>*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorStdDevSamp>*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorSum>*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorMergeObjects>*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionTests::Testable*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionRandom*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionToHashedIndexKey*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionGetField*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionSetField*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionTsSecond*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionTsIncrement*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalOwningShard*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalIndexKey*) override {
        _tracker.enterEvaluateOrCompare();
    }
    void visit(const ExpressionInternalKeyStringValue*) override {
        _tracker.enterEvaluateOrCompare();
    }


    void visit(const ExpressionCond*) override {
        // We enter evaluate on the first child (if branch), since the result of the expression is
        // always coerced to bool and not returned to the caller. The exit occurs between visiting
        // the 'if' and 'then' children in the InVisitor.
        _tracker.enterEvaluateOrCompare();
    }

    void visit(const ExpressionSwitch*) override {
        _tracker.enterEvaluateOrCompare();
    }

    void visit(const ExpressionFieldPath* expr) override {
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

    void visit(const ExpressionConstant*) override {
        _tracker.reconcileLiteral();
    }

    void visit(const ExpressionIfNull*) override {
        // Do not enter an evaluation subtree for $ifNull since either of the two children could be
        // returned depending on the evaluated result of the first.
    }

    void visit(const ExpressionObject* expr) override {
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

    void visit(const ExpressionAbs*) override {}
    void visit(const ExpressionAdd*) override {}
    void visit(const ExpressionAllElementsTrue*) override {}
    void visit(const ExpressionAnd*) override {}
    void visit(const ExpressionAnyElementTrue*) override {}
    void visit(const ExpressionArray*) override {}
    void visit(const ExpressionArrayElemAt*) override {}
    void visit(const ExpressionBitAnd*) override {}
    void visit(const ExpressionBitOr*) override {}
    void visit(const ExpressionBitXor*) override {}
    void visit(const ExpressionBitNot*) override {}
    void visit(const ExpressionFirst*) override {}
    void visit(const ExpressionLast*) override {}
    void visit(const ExpressionObjectToArray*) override {}
    void visit(const ExpressionArrayToObject*) override {}
    void visit(const ExpressionBsonSize*) override {}
    void visit(const ExpressionCeil*) override {}
    void visit(const ExpressionCoerceToBool*) override {}
    void visit(const ExpressionCompare*) override {}
    void visit(const ExpressionConcat*) override {}
    void visit(const ExpressionConcatArrays*) override {}
    void visit(const ExpressionConstant*) override {}
    void visit(const ExpressionDateAdd*) override {}
    void visit(const ExpressionDateDiff*) override {}
    void visit(const ExpressionDateFromString*) override {}
    void visit(const ExpressionDateFromParts*) override {}
    void visit(const ExpressionDateSubtract*) override {}
    void visit(const ExpressionDateToParts*) override {}
    void visit(const ExpressionDateToString*) override {}
    void visit(const ExpressionDateTrunc*) override {}
    void visit(const ExpressionDivide*) override {}
    void visit(const ExpressionExp*) override {}
    void visit(const ExpressionFilter*) override {}
    void visit(const ExpressionFloor*) override {}
    void visit(const ExpressionFunction*) override {}
    void visit(const ExpressionIfNull*) override {}
    void visit(const ExpressionIn*) override {}
    void visit(const ExpressionIndexOfArray*) override {}
    void visit(const ExpressionIndexOfBytes*) override {}
    void visit(const ExpressionIndexOfCP*) override {}
    void visit(const ExpressionInternalJsEmit*) override {}
    void visit(const ExpressionInternalFindElemMatch*) override {}
    void visit(const ExpressionInternalFindPositional*) override {}
    void visit(const ExpressionInternalFindSlice*) override {}
    void visit(const ExpressionIsNumber*) override {}
    void visit(const ExpressionLet*) override {}
    void visit(const ExpressionLn*) override {}
    void visit(const ExpressionLog*) override {}
    void visit(const ExpressionLog10*) override {}
    void visit(const ExpressionInternalFLEEqual*) override {}
    void visit(const ExpressionInternalFLEBetween*) override {}
    void visit(const ExpressionInternalRawSortKey*) override {}
    void visit(const ExpressionMap*) override {}
    void visit(const ExpressionMeta*) override {}
    void visit(const ExpressionMod*) override {}
    void visit(const ExpressionMultiply*) override {}
    void visit(const ExpressionNot*) override {}
    void visit(const ExpressionObject*) override {}
    void visit(const ExpressionOr*) override {}
    void visit(const ExpressionPow*) override {}
    void visit(const ExpressionRange*) override {}
    void visit(const ExpressionReduce*) override {}
    void visit(const ExpressionReplaceOne*) override {}
    void visit(const ExpressionReplaceAll*) override {}
    void visit(const ExpressionSetDifference*) override {}
    void visit(const ExpressionSetEquals*) override {}
    void visit(const ExpressionSetIntersection*) override {}
    void visit(const ExpressionSetIsSubset*) override {}
    void visit(const ExpressionSetUnion*) override {}
    void visit(const ExpressionSize*) override {}
    void visit(const ExpressionReverseArray*) override {}
    void visit(const ExpressionSortArray*) override {}
    void visit(const ExpressionSlice*) override {}
    void visit(const ExpressionIsArray*) override {}
    void visit(const ExpressionInternalFindAllValuesAtPath*) override {}
    void visit(const ExpressionRound*) override {}
    void visit(const ExpressionSplit*) override {}
    void visit(const ExpressionSqrt*) override {}
    void visit(const ExpressionStrcasecmp*) override {}
    void visit(const ExpressionSubstrBytes*) override {}
    void visit(const ExpressionSubstrCP*) override {}
    void visit(const ExpressionStrLenBytes*) override {}
    void visit(const ExpressionBinarySize*) override {}
    void visit(const ExpressionStrLenCP*) override {}
    void visit(const ExpressionSubtract*) override {}
    void visit(const ExpressionTestApiVersion*) override {}
    void visit(const ExpressionToLower*) override {}
    void visit(const ExpressionToUpper*) override {}
    void visit(const ExpressionTrim*) override {}
    void visit(const ExpressionTrunc*) override {}
    void visit(const ExpressionType*) override {}
    void visit(const ExpressionZip*) override {}
    void visit(const ExpressionConvert*) override {}
    void visit(const ExpressionRegexFind*) override {}
    void visit(const ExpressionRegexFindAll*) override {}
    void visit(const ExpressionRegexMatch*) override {}
    void visit(const ExpressionCosine*) override {}
    void visit(const ExpressionSine*) override {}
    void visit(const ExpressionTangent*) override {}
    void visit(const ExpressionArcCosine*) override {}
    void visit(const ExpressionArcSine*) override {}
    void visit(const ExpressionArcTangent*) override {}
    void visit(const ExpressionArcTangent2*) override {}
    void visit(const ExpressionHyperbolicArcTangent*) override {}
    void visit(const ExpressionHyperbolicArcCosine*) override {}
    void visit(const ExpressionHyperbolicArcSine*) override {}
    void visit(const ExpressionHyperbolicTangent*) override {}
    void visit(const ExpressionHyperbolicCosine*) override {}
    void visit(const ExpressionHyperbolicSine*) override {}
    void visit(const ExpressionDegreesToRadians*) override {}
    void visit(const ExpressionRadiansToDegrees*) override {}
    void visit(const ExpressionDayOfMonth*) override {}
    void visit(const ExpressionDayOfWeek*) override {}
    void visit(const ExpressionDayOfYear*) override {}
    void visit(const ExpressionHour*) override {}
    void visit(const ExpressionMillisecond*) override {}
    void visit(const ExpressionMinute*) override {}
    void visit(const ExpressionMonth*) override {}
    void visit(const ExpressionSecond*) override {}
    void visit(const ExpressionWeek*) override {}
    void visit(const ExpressionIsoWeekYear*) override {}
    void visit(const ExpressionIsoDayOfWeek*) override {}
    void visit(const ExpressionIsoWeek*) override {}
    void visit(const ExpressionYear*) override {}
    void visit(const ExpressionFromAccumulator<AccumulatorAvg>*) override {}
    void visit(const ExpressionFromAccumulatorN<AccumulatorFirstN>*) override {}
    void visit(const ExpressionFromAccumulatorN<AccumulatorLastN>*) override {}
    void visit(const ExpressionFromAccumulator<AccumulatorMax>*) override {}
    void visit(const ExpressionFromAccumulator<AccumulatorMin>*) override {}
    void visit(const ExpressionFromAccumulatorN<AccumulatorMaxN>*) override {}
    void visit(const ExpressionFromAccumulatorN<AccumulatorMinN>*) override {}
    void visit(const ExpressionFromAccumulatorQuantile<AccumulatorMedian>*) override {}
    void visit(const ExpressionFromAccumulatorQuantile<AccumulatorPercentile>*) override {}
    void visit(const ExpressionFromAccumulator<AccumulatorStdDevPop>*) override {}
    void visit(const ExpressionFromAccumulator<AccumulatorStdDevSamp>*) override {}
    void visit(const ExpressionFromAccumulator<AccumulatorSum>*) override {}
    void visit(const ExpressionFromAccumulator<AccumulatorMergeObjects>*) override {}
    void visit(const ExpressionTests::Testable*) override {}
    void visit(const ExpressionFieldPath*) override {}
    void visit(const ExpressionRandom*) override {}
    void visit(const ExpressionToHashedIndexKey*) override {}
    void visit(const ExpressionGetField*) override {}
    void visit(const ExpressionSetField*) override {}
    void visit(const ExpressionTsSecond*) override {}
    void visit(const ExpressionTsIncrement*) override {}
    void visit(const ExpressionInternalOwningShard*) override {}
    void visit(const ExpressionInternalIndexKey*) override {}
    void visit(const ExpressionInternalKeyStringValue*) override {}


    void visit(const ExpressionCond*) override {
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

    void visit(const ExpressionSwitch* switchExpr) override {
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

    void visit(const ExpressionAbs*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionAdd*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionAllElementsTrue*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionAnd*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionAnyElementTrue*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionArray*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionArrayElemAt*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionBitAnd*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionBitOr*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionBitXor*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionBitNot*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFirst*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionLast*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionObjectToArray*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionArrayToObject*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionBsonSize*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionCeil*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionCoerceToBool*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionCompare*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionConcat*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionConcatArrays*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateAdd*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateDiff*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateFromString*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateFromParts*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateSubtract*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateToParts*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateToString*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDateTrunc*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDivide*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionExp*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFilter*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFloor*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFunction*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIn*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIndexOfArray*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIndexOfBytes*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIndexOfCP*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalJsEmit*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindElemMatch*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindPositional*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindSlice*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIsNumber*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionLet*) override {}
    void visit(const ExpressionLn*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionLog*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionLog10*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFLEEqual*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFLEBetween*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalRawSortKey*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionMap*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionMeta*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionMod*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionMultiply*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionNot*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionOr*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionPow*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionRange*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionReduce*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionReplaceOne*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionReplaceAll*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSetDifference*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSetEquals*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSetIntersection*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSetIsSubset*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSetUnion*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSize*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionReverseArray*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSortArray*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSlice*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIsArray*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalFindAllValuesAtPath*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionRound*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSplit*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSqrt*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionStrcasecmp*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSubstrBytes*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSubstrCP*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionStrLenBytes*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionBinarySize*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionStrLenCP*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSubtract*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionTestApiVersion*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionToLower*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionToUpper*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionTrim*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionTrunc*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionType*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionZip*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionConvert*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionRegexFind*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionRegexFindAll*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionRegexMatch*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionCosine*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSine*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionTangent*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionArcCosine*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionArcSine*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionArcTangent*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionArcTangent2*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicArcTangent*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicArcCosine*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicArcSine*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicTangent*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicCosine*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionHyperbolicSine*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDegreesToRadians*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionRadiansToDegrees*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDayOfMonth*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDayOfWeek*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionDayOfYear*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionHour*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionMillisecond*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionMinute*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionMonth*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSecond*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionWeek*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIsoWeekYear*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIsoDayOfWeek*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionIsoWeek*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionYear*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorAvg>*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorFirstN>*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorLastN>*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorMax>*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorMin>*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorMaxN>*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorN<AccumulatorMinN>*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorQuantile<AccumulatorMedian>*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulatorQuantile<AccumulatorPercentile>*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorStdDevPop>*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorStdDevSamp>*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorSum>*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionFromAccumulator<AccumulatorMergeObjects>*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionTests::Testable*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionRandom*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionToHashedIndexKey*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionGetField*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionSetField*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionTsSecond*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionTsIncrement*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalOwningShard*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalIndexKey*) override {
        _tracker.exitEvaluateOrCompare();
    }
    void visit(const ExpressionInternalKeyStringValue*) override {
        _tracker.exitEvaluateOrCompare();
    }


    // These expressions are not considered evaluated, however the interesting logic is handling in
    // the {Pre/In}Visitors.
    void visit(const ExpressionFieldPath*) override {}
    void visit(const ExpressionConstant*) override {}
    void visit(const ExpressionCond*) override {}
    void visit(const ExpressionSwitch*) override {}
    void visit(const ExpressionIfNull*) override {}
    void visit(const ExpressionObject*) override {
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
