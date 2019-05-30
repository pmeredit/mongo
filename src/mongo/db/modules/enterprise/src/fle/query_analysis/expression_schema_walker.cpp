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
 * Visitor which is called on the 'preVisit' for a given expression.
 */
class ExpressionSchemaPreVisitor : public ExpressionVisitor {
public:
    ExpressionSchemaPreVisitor(const EncryptionSchemaTreeNode& schema,
                               unsigned long long& evaluateSubtreeCount)
        : _schema(schema), _evaluateSubtreeCount(evaluateSubtreeCount) {}

    void visit(ExpressionAbs*) {
        enterEvaluate();
    }
    void visit(ExpressionAdd*) {
        enterEvaluate();
    }
    void visit(ExpressionAllElementsTrue*) {
        enterEvaluate();
    }
    void visit(ExpressionAnd*) {
        enterEvaluate();
    }
    void visit(ExpressionAnyElementTrue*) {
        enterEvaluate();
    }
    void visit(ExpressionArray*) {
        enterEvaluate();
    }
    void visit(ExpressionArrayElemAt*) {
        enterEvaluate();
    }
    void visit(ExpressionObjectToArray*) {
        enterEvaluate();
    }
    void visit(ExpressionArrayToObject*) {
        enterEvaluate();
    }
    void visit(ExpressionCeil*) {
        enterEvaluate();
    }
    void visit(ExpressionCoerceToBool*) {
        enterEvaluate();
    }
    void visit(ExpressionCompare*) {
        enterEvaluate();
    }
    void visit(ExpressionConcat*) {
        enterEvaluate();
    }
    void visit(ExpressionConcatArrays*) {
        enterEvaluate();
    }
    void visit(ExpressionCond*) {
        throwNotSupported("$cond");
    }
    void visit(ExpressionDateFromString*) {
        enterEvaluate();
    }
    void visit(ExpressionDateFromParts*) {
        enterEvaluate();
    }
    void visit(ExpressionDateToParts*) {
        enterEvaluate();
    }
    void visit(ExpressionDateToString*) {
        enterEvaluate();
    }
    void visit(ExpressionDivide*) {
        enterEvaluate();
    }
    void visit(ExpressionExp*) {
        enterEvaluate();
    }
    void visit(ExpressionFilter*) {
        enterEvaluate();
    }
    void visit(ExpressionFloor*) {
        enterEvaluate();
    }
    void visit(ExpressionIfNull*) {
        enterEvaluate();
    }
    void visit(ExpressionIn*) {
        enterEvaluate();
    }
    void visit(ExpressionIndexOfArray*) {
        enterEvaluate();
    }
    void visit(ExpressionIndexOfBytes*) {
        enterEvaluate();
    }
    void visit(ExpressionIndexOfCP*) {
        enterEvaluate();
    }
    void visit(ExpressionLet*) {
        throwNotSupported("$let");
    }
    void visit(ExpressionLn*) {
        enterEvaluate();
    }
    void visit(ExpressionLog*) {
        enterEvaluate();
    }
    void visit(ExpressionLog10*) {
        enterEvaluate();
    }
    void visit(ExpressionMap*) {
        enterEvaluate();
    }
    void visit(ExpressionMeta*) {
        enterEvaluate();
    }
    void visit(ExpressionMod*) {
        enterEvaluate();
    }
    void visit(ExpressionMultiply*) {
        enterEvaluate();
    }
    void visit(ExpressionNot*) {
        enterEvaluate();
    }
    void visit(ExpressionObject*) {
        throwNotSupported("Object expressions");
    }
    void visit(ExpressionOr*) {
        enterEvaluate();
    }
    void visit(ExpressionPow*) {
        enterEvaluate();
    }
    void visit(ExpressionRange*) {
        enterEvaluate();
    }
    void visit(ExpressionReduce*) {
        enterEvaluate();
    }
    void visit(ExpressionSetDifference*) {
        enterEvaluate();
    }
    void visit(ExpressionSetEquals*) {
        enterEvaluate();
    }
    void visit(ExpressionSetIntersection*) {
        enterEvaluate();
    }
    void visit(ExpressionSetIsSubset*) {
        enterEvaluate();
    }
    void visit(ExpressionSetUnion*) {
        enterEvaluate();
    }
    void visit(ExpressionSize*) {
        enterEvaluate();
    }
    void visit(ExpressionReverseArray*) {
        enterEvaluate();
    }
    void visit(ExpressionSlice*) {
        enterEvaluate();
    }
    void visit(ExpressionIsArray*) {
        enterEvaluate();
    }
    void visit(ExpressionRound*) {
        enterEvaluate();
    }
    void visit(ExpressionSplit*) {
        enterEvaluate();
    }
    void visit(ExpressionSqrt*) {
        enterEvaluate();
    }
    void visit(ExpressionStrcasecmp*) {
        enterEvaluate();
    }
    void visit(ExpressionSubstrBytes*) {
        enterEvaluate();
    }
    void visit(ExpressionSubstrCP*) {
        enterEvaluate();
    }
    void visit(ExpressionStrLenBytes*) {
        enterEvaluate();
    }
    void visit(ExpressionStrLenCP*) {
        enterEvaluate();
    }
    void visit(ExpressionSubtract*) {
        enterEvaluate();
    }
    void visit(ExpressionSwitch*) {
        throwNotSupported("$switch");
    }
    void visit(ExpressionToLower*) {
        enterEvaluate();
    }
    void visit(ExpressionToUpper*) {
        enterEvaluate();
    }
    void visit(ExpressionTrim*) {
        enterEvaluate();
    }
    void visit(ExpressionTrunc*) {
        enterEvaluate();
    }
    void visit(ExpressionType*) {
        enterEvaluate();
    }
    void visit(ExpressionZip*) {
        enterEvaluate();
    }
    void visit(ExpressionConvert*) {
        enterEvaluate();
    }
    void visit(ExpressionRegexFind*) {
        enterEvaluate();
    }
    void visit(ExpressionRegexFindAll*) {
        enterEvaluate();
    }
    void visit(ExpressionRegexMatch*) {
        enterEvaluate();
    }
    void visit(ExpressionCosine*) {
        enterEvaluate();
    }
    void visit(ExpressionSine*) {
        enterEvaluate();
    }
    void visit(ExpressionTangent*) {
        enterEvaluate();
    }
    void visit(ExpressionArcCosine*) {
        enterEvaluate();
    }
    void visit(ExpressionArcSine*) {
        enterEvaluate();
    }
    void visit(ExpressionArcTangent*) {
        enterEvaluate();
    }
    void visit(ExpressionArcTangent2*) {
        enterEvaluate();
    }
    void visit(ExpressionHyperbolicArcTangent*) {
        enterEvaluate();
    }
    void visit(ExpressionHyperbolicArcCosine*) {
        enterEvaluate();
    }
    void visit(ExpressionHyperbolicArcSine*) {
        enterEvaluate();
    }
    void visit(ExpressionHyperbolicTangent*) {
        enterEvaluate();
    }
    void visit(ExpressionHyperbolicCosine*) {
        enterEvaluate();
    }
    void visit(ExpressionHyperbolicSine*) {
        enterEvaluate();
    }
    void visit(ExpressionDegreesToRadians*) {
        enterEvaluate();
    }
    void visit(ExpressionRadiansToDegrees*) {
        enterEvaluate();
    }
    void visit(ExpressionDayOfMonth*) {
        enterEvaluate();
    }
    void visit(ExpressionDayOfWeek*) {
        enterEvaluate();
    }
    void visit(ExpressionDayOfYear*) {
        enterEvaluate();
    }
    void visit(ExpressionHour*) {
        enterEvaluate();
    }
    void visit(ExpressionMillisecond*) {
        enterEvaluate();
    }
    void visit(ExpressionMinute*) {
        enterEvaluate();
    }
    void visit(ExpressionMonth*) {
        enterEvaluate();
    }
    void visit(ExpressionSecond*) {
        enterEvaluate();
    }
    void visit(ExpressionWeek*) {
        enterEvaluate();
    }
    void visit(ExpressionIsoWeekYear*) {
        enterEvaluate();
    }
    void visit(ExpressionIsoDayOfWeek*) {
        enterEvaluate();
    }
    void visit(ExpressionIsoWeek*) {
        enterEvaluate();
    }
    void visit(ExpressionYear*) {
        enterEvaluate();
    }
    void visit(ExpressionFromAccumulator<AccumulatorAvg>*) {
        enterEvaluate();
    }
    void visit(ExpressionFromAccumulator<AccumulatorMax>*) {
        enterEvaluate();
    }
    void visit(ExpressionFromAccumulator<AccumulatorMin>*) {
        enterEvaluate();
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>*) {
        enterEvaluate();
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>*) {
        enterEvaluate();
    }
    void visit(ExpressionFromAccumulator<AccumulatorSum>*) {
        enterEvaluate();
    }
    void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>*) {
        enterEvaluate();
    }
    void visit(ExpressionTests::Testable*) {
        enterEvaluate();
    }

    void visit(ExpressionFieldPath* expr) {
        auto fieldPath = expr->getFieldPath();
        uassert(31127,
                str::stream() << "Variables in aggregation expressions are not supported with "
                                 "client-side encryption",
                fieldPath.getFieldName(0) == "CURRENT" && fieldPath.getPathLength() > 1);

        FieldRef path{fieldPath.tail().fullPath()};

        if (auto node = _schema.getNode(path)) {
            reconcileSchema(node->clone());
        } else {
            reconcileSchema(std::make_unique<EncryptionSchemaNotEncryptedNode>());
        }
    }

    void visit(ExpressionConstant*) {
        reconcileSchema(std::make_unique<EncryptionSchemaNotEncryptedNode>());
    }

    std::unique_ptr<EncryptionSchemaTreeNode> releaseOutputSchema() {
        return std::move(_outputSchema);
    }

private:
    void throwNotSupported(std::string expr) {
        uasserted(31128, str::stream() << expr << " not supported with client-side encryption");
    }

    /**
     * Attempts to reconcile the 'newSchema' with the current output schema stored in _schema. If
     * called within an evaluated expression (_evaluateSubtreeCount > 0), then this method does
     * nothing.
     */
    void reconcileSchema(std::unique_ptr<EncryptionSchemaTreeNode> newSchema) {
        if (_evaluateSubtreeCount == 0) {
            uassert(51203,
                    "Cannot mix an expression which evaluates to an encrypted value with one that "
                    "evaluates to a non-encrypted value",
                    !_outputSchema ||
                        _outputSchema->getEncryptionMetadata() ==
                            newSchema->getEncryptionMetadata());
            if (!_outputSchema)
                _outputSchema = std::move(newSchema);
        }
    }

    /**
     * Entering an "evaluation" expression means that the result is always not-encrypted. However,
     * we still keep track of the current nesting level to make sure that we only set the output
     * schema when exiting the outermost evaluated expression.
     */
    void enterEvaluate() {
        reconcileSchema(std::make_unique<EncryptionSchemaNotEncryptedNode>());
        ++_evaluateSubtreeCount;
    }

    const EncryptionSchemaTreeNode& _schema;
    unsigned long long& _evaluateSubtreeCount;
    std::unique_ptr<EncryptionSchemaTreeNode> _outputSchema;
};

/**
 * Visitor which is called on the 'inVisit' for a given expression.
 */
class ExpressionSchemaInVisitor : public ExpressionVisitor {
public:
    ExpressionSchemaInVisitor(const EncryptionSchemaTreeNode& schema,
                              unsigned long long& evaluateSubtreeCount)
        : _schema(schema), _evaluateSubtreeCount(evaluateSubtreeCount) {}

    void visit(ExpressionAbs*) {}
    void visit(ExpressionAdd*) {}
    void visit(ExpressionAllElementsTrue*) {}
    void visit(ExpressionAnd*) {}
    void visit(ExpressionAnyElementTrue*) {}
    void visit(ExpressionArray*) {}
    void visit(ExpressionArrayElemAt*) {}
    void visit(ExpressionObjectToArray*) {}
    void visit(ExpressionArrayToObject*) {}
    void visit(ExpressionCeil*) {}
    void visit(ExpressionCoerceToBool*) {}
    void visit(ExpressionCompare*) {}
    void visit(ExpressionConcat*) {}
    void visit(ExpressionConcatArrays*) {}
    void visit(ExpressionCond*) {}
    void visit(ExpressionConstant*) {}
    void visit(ExpressionDateFromString*) {}
    void visit(ExpressionDateFromParts*) {}
    void visit(ExpressionDateToParts*) {}
    void visit(ExpressionDateToString*) {}
    void visit(ExpressionDivide*) {}
    void visit(ExpressionExp*) {}
    void visit(ExpressionFilter*) {}
    void visit(ExpressionFloor*) {}
    void visit(ExpressionIfNull*) {}
    void visit(ExpressionIn*) {}
    void visit(ExpressionIndexOfArray*) {}
    void visit(ExpressionIndexOfBytes*) {}
    void visit(ExpressionIndexOfCP*) {}
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
    void visit(ExpressionStrLenCP*) {}
    void visit(ExpressionSubtract*) {}
    void visit(ExpressionSwitch*) {}
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

    // Used to indicate which child expression is "in visit", since expressions hold their children
    // with an index into a vector.
    unsigned long long count;

private:
    const EncryptionSchemaTreeNode& _schema;
    unsigned long long& _evaluateSubtreeCount;
};

/**
 * Visitor which is called on the 'postVisit' for a given expression.
 */
class ExpressionSchemaPostVisitor : public ExpressionVisitor {
public:
    ExpressionSchemaPostVisitor(const EncryptionSchemaTreeNode& schema,
                                unsigned long long& evaluateSubtreeCount)
        : _schema(schema), _evaluateSubtreeCount(evaluateSubtreeCount) {}

    void visit(ExpressionAbs*) {
        exitEvaluate();
    }
    void visit(ExpressionAdd*) {
        exitEvaluate();
    }
    void visit(ExpressionAllElementsTrue*) {
        exitEvaluate();
    }
    void visit(ExpressionAnd*) {
        exitEvaluate();
    }
    void visit(ExpressionAnyElementTrue*) {
        exitEvaluate();
    }
    void visit(ExpressionArray*) {
        exitEvaluate();
    }
    void visit(ExpressionArrayElemAt*) {
        exitEvaluate();
    }
    void visit(ExpressionObjectToArray*) {
        exitEvaluate();
    }
    void visit(ExpressionArrayToObject*) {
        exitEvaluate();
    }
    void visit(ExpressionCeil*) {
        exitEvaluate();
    }
    void visit(ExpressionCoerceToBool*) {
        exitEvaluate();
    }
    void visit(ExpressionCompare*) {
        exitEvaluate();
    }
    void visit(ExpressionConcat*) {
        exitEvaluate();
    }
    void visit(ExpressionConcatArrays*) {
        exitEvaluate();
    }
    void visit(ExpressionCond*) {
        invariant(0);
    }
    void visit(ExpressionDateFromString*) {
        exitEvaluate();
    }
    void visit(ExpressionDateFromParts*) {
        exitEvaluate();
    }
    void visit(ExpressionDateToParts*) {
        exitEvaluate();
    }
    void visit(ExpressionDateToString*) {
        exitEvaluate();
    }
    void visit(ExpressionDivide*) {
        exitEvaluate();
    }
    void visit(ExpressionExp*) {
        exitEvaluate();
    }
    void visit(ExpressionFilter*) {
        exitEvaluate();
    }
    void visit(ExpressionFloor*) {
        exitEvaluate();
    }
    void visit(ExpressionIfNull*) {
        exitEvaluate();
    }
    void visit(ExpressionIn*) {
        exitEvaluate();
    }
    void visit(ExpressionIndexOfArray*) {
        exitEvaluate();
    }
    void visit(ExpressionIndexOfBytes*) {
        exitEvaluate();
    }
    void visit(ExpressionIndexOfCP*) {
        exitEvaluate();
    }
    void visit(ExpressionLet*) {
        exitEvaluate();
    }
    void visit(ExpressionLn*) {
        exitEvaluate();
    }
    void visit(ExpressionLog*) {
        exitEvaluate();
    }
    void visit(ExpressionLog10*) {
        exitEvaluate();
    }
    void visit(ExpressionMap*) {
        exitEvaluate();
    }
    void visit(ExpressionMeta*) {
        exitEvaluate();
    }
    void visit(ExpressionMod*) {
        exitEvaluate();
    }
    void visit(ExpressionMultiply*) {
        exitEvaluate();
    }
    void visit(ExpressionNot*) {
        exitEvaluate();
    }
    void visit(ExpressionObject*) {
        invariant(0);
    }
    void visit(ExpressionOr*) {
        exitEvaluate();
    }
    void visit(ExpressionPow*) {
        exitEvaluate();
    }
    void visit(ExpressionRange*) {
        exitEvaluate();
    }
    void visit(ExpressionReduce*) {
        exitEvaluate();
    }
    void visit(ExpressionSetDifference*) {
        exitEvaluate();
    }
    void visit(ExpressionSetEquals*) {
        exitEvaluate();
    }
    void visit(ExpressionSetIntersection*) {
        exitEvaluate();
    }
    void visit(ExpressionSetIsSubset*) {
        exitEvaluate();
    }
    void visit(ExpressionSetUnion*) {
        exitEvaluate();
    }
    void visit(ExpressionSize*) {
        exitEvaluate();
    }
    void visit(ExpressionReverseArray*) {
        exitEvaluate();
    }
    void visit(ExpressionSlice*) {
        exitEvaluate();
    }
    void visit(ExpressionIsArray*) {
        exitEvaluate();
    }
    void visit(ExpressionRound*) {
        exitEvaluate();
    }
    void visit(ExpressionSplit*) {
        exitEvaluate();
    }
    void visit(ExpressionSqrt*) {
        exitEvaluate();
    }
    void visit(ExpressionStrcasecmp*) {
        exitEvaluate();
    }
    void visit(ExpressionSubstrBytes*) {
        exitEvaluate();
    }
    void visit(ExpressionSubstrCP*) {
        exitEvaluate();
    }
    void visit(ExpressionStrLenBytes*) {
        exitEvaluate();
    }
    void visit(ExpressionStrLenCP*) {
        exitEvaluate();
    }
    void visit(ExpressionSubtract*) {
        exitEvaluate();
    }
    void visit(ExpressionSwitch*) {
        invariant(0);
    }
    void visit(ExpressionToLower*) {
        exitEvaluate();
    }
    void visit(ExpressionToUpper*) {
        exitEvaluate();
    }
    void visit(ExpressionTrim*) {
        exitEvaluate();
    }
    void visit(ExpressionTrunc*) {
        exitEvaluate();
    }
    void visit(ExpressionType*) {
        exitEvaluate();
    }
    void visit(ExpressionZip*) {
        exitEvaluate();
    }
    void visit(ExpressionConvert*) {
        exitEvaluate();
    }
    void visit(ExpressionRegexFind*) {
        exitEvaluate();
    }
    void visit(ExpressionRegexFindAll*) {
        exitEvaluate();
    }
    void visit(ExpressionRegexMatch*) {
        exitEvaluate();
    }
    void visit(ExpressionCosine*) {
        exitEvaluate();
    }
    void visit(ExpressionSine*) {
        exitEvaluate();
    }
    void visit(ExpressionTangent*) {
        exitEvaluate();
    }
    void visit(ExpressionArcCosine*) {
        exitEvaluate();
    }
    void visit(ExpressionArcSine*) {
        exitEvaluate();
    }
    void visit(ExpressionArcTangent*) {
        exitEvaluate();
    }
    void visit(ExpressionArcTangent2*) {
        exitEvaluate();
    }
    void visit(ExpressionHyperbolicArcTangent*) {
        exitEvaluate();
    }
    void visit(ExpressionHyperbolicArcCosine*) {
        exitEvaluate();
    }
    void visit(ExpressionHyperbolicArcSine*) {
        exitEvaluate();
    }
    void visit(ExpressionHyperbolicTangent*) {
        exitEvaluate();
    }
    void visit(ExpressionHyperbolicCosine*) {
        exitEvaluate();
    }
    void visit(ExpressionHyperbolicSine*) {
        exitEvaluate();
    }
    void visit(ExpressionDegreesToRadians*) {
        exitEvaluate();
    }
    void visit(ExpressionRadiansToDegrees*) {
        exitEvaluate();
    }
    void visit(ExpressionDayOfMonth*) {
        exitEvaluate();
    }
    void visit(ExpressionDayOfWeek*) {
        exitEvaluate();
    }
    void visit(ExpressionDayOfYear*) {
        exitEvaluate();
    }
    void visit(ExpressionHour*) {
        exitEvaluate();
    }
    void visit(ExpressionMillisecond*) {
        exitEvaluate();
    }
    void visit(ExpressionMinute*) {
        exitEvaluate();
    }
    void visit(ExpressionMonth*) {
        exitEvaluate();
    }
    void visit(ExpressionSecond*) {
        exitEvaluate();
    }
    void visit(ExpressionWeek*) {
        exitEvaluate();
    }
    void visit(ExpressionIsoWeekYear*) {
        exitEvaluate();
    }
    void visit(ExpressionIsoDayOfWeek*) {
        exitEvaluate();
    }
    void visit(ExpressionIsoWeek*) {
        exitEvaluate();
    }
    void visit(ExpressionYear*) {
        exitEvaluate();
    }
    void visit(ExpressionFromAccumulator<AccumulatorAvg>*) {
        exitEvaluate();
    }
    void visit(ExpressionFromAccumulator<AccumulatorMax>*) {
        exitEvaluate();
    }
    void visit(ExpressionFromAccumulator<AccumulatorMin>*) {
        exitEvaluate();
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>*) {
        exitEvaluate();
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>*) {
        exitEvaluate();
    }
    void visit(ExpressionFromAccumulator<AccumulatorSum>*) {
        exitEvaluate();
    }
    void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>*) {
        exitEvaluate();
    }
    void visit(ExpressionTests::Testable*) {
        exitEvaluate();
    }

    void visit(ExpressionFieldPath*) {}

    void visit(ExpressionConstant*) {}

private:
    void exitEvaluate() {
        invariant(_evaluateSubtreeCount > 0);
        --_evaluateSubtreeCount;
    }

    const EncryptionSchemaTreeNode& _schema;
    unsigned long long& _evaluateSubtreeCount;
};

/**
 * ExpressionWalker which takes an input schema and returns a new schema representing the result of
 * evaluating the expression being walked.
 */
class ExpressionWalkerSchema {
public:
    ExpressionWalkerSchema(const EncryptionSchemaTreeNode& schema)
        : _schema(schema),
          _evaluateSubtreeCount(0),
          _preVisitor(_schema, _evaluateSubtreeCount),
          _inVisitor(_schema, _evaluateSubtreeCount),
          _postVisitor(_schema, _evaluateSubtreeCount) {}

    void preVisit(Expression* expr) {
        expr->acceptVisitor(&_preVisitor);
    }
    void inVisit(unsigned long long count, Expression* expr) {
        _inVisitor.count = count;
        expr->acceptVisitor(&_inVisitor);
    }
    void postVisit(Expression* expr) {
        expr->acceptVisitor(&_postVisitor);
    }

    std::unique_ptr<EncryptionSchemaTreeNode> releaseOutputSchema() {
        return _preVisitor.releaseOutputSchema();
    }

private:
    const EncryptionSchemaTreeNode& _schema;

    // Tracks the evaluated nesting level of the current expression being visited. In general,
    // there's no work to be done within an evaluated expression (e.g. $add) since the result will
    // always be non-encrypted. However, we must track the depth of the evaluation to know when
    // we've exited or called 'postVisit' on root of the evaluation sub-tree.
    unsigned long long _evaluateSubtreeCount{0};

    ExpressionSchemaPreVisitor _preVisitor;
    ExpressionSchemaInVisitor _inVisitor;
    ExpressionSchemaPostVisitor _postVisitor;
};

}  // namespace

std::unique_ptr<EncryptionSchemaTreeNode> getOutputSchema(const EncryptionSchemaTreeNode& schema,
                                                          Expression* expression) {
    ExpressionWalkerSchema schemaWalker{schema};
    expression_walker::walk(&schemaWalker, expression);
    return schemaWalker.releaseOutputSchema();
}

}  // namespace mongo::aggregate_expression_intender
