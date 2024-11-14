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

#include "agg_expression_encryption_intender_base.h"
#include "mongo/base/string_data.h"
#include "mongo/db/pipeline/expression_visitor.h"
#include "mongo/db/pipeline/expression_walker.h"
#include "query_analysis.h"
#include "resolved_encryption_info.h"

namespace mongo::aggregate_expression_intender {


using namespace fmt::literals;
using namespace std::string_literals;

std::string toString(const decltype(Subtree::output)& outputType) {
    return visit(
        [&](auto&& outputType) {
            using OutputType = std::decay_t<decltype(outputType)>;
            if constexpr (std::is_same_v<OutputType, Subtree::Forwarded>) {
                return "Subtree::Forwarded";
            } else if constexpr (std::is_same_v<OutputType, Subtree::Compared>) {
                return "Subtree::Compared";
            } else if constexpr (std::is_same_v<OutputType, Subtree::Evaluated>) {
                return "Subtree::Evaluated";
            }
        },
        outputType);
}

void rewriteLiteralToIntent(const ExpressionContext& expCtx,
                            const ResolvedEncryptionInfo& encryptedType,
                            ExpressionConstant* literal) {
    using namespace query_analysis;
    auto constVal = literal->getValue();
    if (isEncryptedPayload(constVal)) {
        // This field path was encrypted by a different walker.
        return;
    }
    literal->setValue(buildEncryptPlaceholder(
        constVal, encryptedType, EncryptionPlaceholderContext::kComparison, expCtx.getCollator()));
}

void enterSubtree(decltype(Subtree::output) outputType, std::stack<Subtree>& subtreeStack) {
    subtreeStack.push({outputType});
}

[[noreturn]] void uassertedEncryptedInEvaluatedContext(const FieldPath& currentField,
                                                       const StringData evaluatedBy) {
    uasserted(31110,
              "Encrypted field '"s + currentField.fullPath() +
                  "' is not allowed to be evaluated by " + evaluatedBy);
}

[[noreturn]] void uassertedEncryptedUnencryptedMismatch(
    const FieldPath& currentField,
    const std::vector<FieldPath>& comparedFields,
    const std::vector<StringData> comparedEvaluations) {
    uasserted(
        31098,
        "Comparison disallowed between encrypted fields and unencrypted fields; '"s +
            currentField.fullPath() + "' is encrypted but is compared to" +
            std::accumulate(comparedFields.begin(),
                            comparedFields.end(),
                            ""s,
                            [](auto&& l, auto&& r) { return l + " '" + r.fullPath() + "'"; }) +
            std::accumulate(comparedEvaluations.begin(),
                            comparedEvaluations.end(),
                            ""s,
                            [](auto&& l, auto&& r) { return l + " result of " + r; }));
}

[[noreturn]] void uassertedUnencryptedEncryptedMismatch(
    const FieldPath& currentField, const std::vector<FieldPath>& comparedFields) {
    uasserted(31099,
              "Comparison disallowed between unencrypted fields and encrypted fields; '"s +
                  currentField.fullPath() + "' is unencrypted but is compared to" +
                  std::accumulate(
                      comparedFields.begin(), comparedFields.end(), ""s, [](auto&& l, auto&& r) {
                          return l + " '" + r.fullPath() + "'";
                      }));
}

[[noreturn]] void uassertedEncryptedEncryptedMismatch(
    const FieldPath& currentField, const std::vector<FieldPath>& comparedFields) {
    uasserted(31100,
              "Comparison disallowed between fields with different encryption algorithms; "
              "encryption algorithm for field '"s +
                  currentField.fullPath() + "' does not match the algorithm of" +
                  std::accumulate(
                      comparedFields.begin(), comparedFields.end(), ""s, [](auto&& l, auto&& r) {
                          return l + " '" + r.fullPath() + "'";
                      }));
}
[[noreturn]] void uassertedComparisonOfRandomlyEncrypted(const FieldPath& currentField) {
    uasserted(31158,
              "Comparison disallowed between fields where one is randomly encrypted; field '"s +
                  currentField.fullPath() + "' is randomly encrypted.");
}
[[noreturn]] void uassertedComparisonFLE2EncryptedFields(const FieldPath& fieldPath0,
                                                         const FieldPath& fieldPath1) {
    uasserted(6334101,
              "Comparison disallowed between two fields encrypted with Queryable Encryption; "
              "fields '"s +
                  fieldPath0.fullPath() + "' and '" + fieldPath1.fullPath() + "' are encrypted.");
}

void ensureFLE2EncryptedFieldComparedToConstant(ExpressionFieldPath* encryptedFieldPath,
                                                Expression* comparedTo) {
    auto constant = dynamic_cast<ExpressionConstant*>(comparedTo);
    uassert(6334105,
            "Comparison disallowed between Queryable Encryption encrypted fields and non-constant "
            "expressions; "
            "field '"s +
                encryptedFieldPath->getFieldPathWithoutCurrentPrefix().fullPath() +
                "' is encrypted.",
            constant);
}

[[noreturn]] void uassertedEvaluationInComparedEncryptedSubtree(
    const StringData evaluation, const std::vector<FieldPath>& comparedFields) {
    uasserted(31117,
              "Result of evaluating "s + evaluation +
                  " forbidden from being compared to encrypted fields but is compared to" +
                  std::accumulate(
                      comparedFields.begin(), comparedFields.end(), ""s, [](auto&& l, auto&& r) {
                          return l + " '" + r.fullPath() + "'";
                      }));
}

[[noreturn]] void uassertedForbiddenVariable(StringData variableName) {
    uasserted(31121, "Access to variable "s + variableName + " disallowed");
}

auto getEncryptionTypeForPathEnsureNotPrefix(const EncryptionSchemaTreeNode& schema,
                                             const ExpressionFieldPath& fieldPath) {
    const auto path = fieldPath.getFieldPathWithoutCurrentPrefix();
    auto encryptedType = schema.getEncryptionMetadataForPath(FieldRef(path.fullPath()));
    // TODO SERVER-41337: Handle the case where a field reference points to the prefix of an
    // encrypted field in a more accepting manner.
    uassert(31131,
            "Found forbidden reference to prefix of encrypted field "s + path.fullPath(),
            encryptedType || !schema.mayContainEncryptedNodeBelowPrefix(FieldRef(path.fullPath())));
    return encryptedType;
}

decltype(Subtree::Compared::state) reconcileAgainstUnknownEncryption(
    const EncryptionSchemaTreeNode& schema, const ExpressionFieldPath& fieldPath) {
    if (auto encryptedType = getEncryptionTypeForPathEnsureNotPrefix(schema, fieldPath))
        // The examined field is encrypted so the current subtree gains our encryption type.
        return Subtree::Compared::Encrypted{std::move(*encryptedType)};
    else
        // The field is unencrypted so we've determined that this Subtree has no encryption.  There
        // is no special error case so leave the reasoning empty.
        return Subtree::Compared::NotEncrypted{};
}

void errorIfEncryptedFieldFoundInEvaluated(const EncryptionSchemaTreeNode& schema,
                                           const ExpressionFieldPath& fieldPath,
                                           Subtree::Evaluated* evaluated) {
    if (getEncryptionTypeForPathEnsureNotPrefix(schema, fieldPath))
        // The examined field is encrypted and the output type of the current Subtree disallows any
        // encrypted fields.
        uassertedEncryptedInEvaluatedContext(fieldPath.getFieldPathWithoutCurrentPrefix(),
                                             evaluated->by);
}

void ensureNotEncryptedEnterEval(const StringData evaluation, std::stack<Subtree>& subtreeStack) {
    ensureNotEncrypted(evaluation, subtreeStack);
    enterSubtree(Subtree::Evaluated{evaluation}, subtreeStack);
}

decltype(Subtree::Compared::state) attemptReconcilingAgainstNoEncryption(
    const EncryptionSchemaTreeNode& schema,
    const ExpressionFieldPath& fieldPath,
    const std::vector<FieldPath>& comparedFields,
    const std::vector<StringData> comparedEvaluations) {
    if (getEncryptionTypeForPathEnsureNotPrefix(schema, fieldPath))
        // The examined field is encrypted but the current Subtree is unencrypted.
        uassertedEncryptedUnencryptedMismatch(
            fieldPath.getFieldPathWithoutCurrentPrefix(), comparedFields, comparedEvaluations);
    else
        // The examined field is unencrypted and so is the current Subtree.
        return Subtree::Compared::NotEncrypted{};
}

decltype(Subtree::Compared::state) attemptReconcilingAgainstEncryption(
    const EncryptionSchemaTreeNode& schema,
    const ExpressionFieldPath& fieldPath,
    const std::vector<FieldPath>& comparedFields,
    const ResolvedEncryptionInfo& currentEncryptedType) {
    if (auto encryptedType = getEncryptionTypeForPathEnsureNotPrefix(schema, fieldPath)) {
        // The examined field is encrypted and so is the current Subtree. The two
        // ResolvedEncryptionInfo instances need to be checked for equality.
        if (encryptedType != currentEncryptedType)
            uassertedEncryptedEncryptedMismatch(fieldPath.getFieldPathWithoutCurrentPrefix(),
                                                comparedFields);
        return Subtree::Compared::Encrypted{std::move(*encryptedType)};
    } else {
        uassertedUnencryptedEncryptedMismatch(fieldPath.getFieldPathWithoutCurrentPrefix(),
                                              comparedFields);
    }
}

void attemptReconcilingFieldEncryptionInCompared(const EncryptionSchemaTreeNode& schema,
                                                 const ExpressionFieldPath& fieldPath,
                                                 Subtree::Compared* compared) {
    // Any reference to a randomly encrypted field within a comparison subtree will fail.
    auto metadata = schema.getEncryptionMetadataForPath(
        FieldRef(fieldPath.getFieldPathWithoutCurrentPrefix().fullPath()));
    if (metadata &&
        (metadata->algorithmIs(FleAlgorithmEnum::kRandom) ||
         metadata->algorithmIs(Fle2AlgorithmInt::kUnindexed))) {
        uassertedComparisonOfRandomlyEncrypted(fieldPath.getFieldPathWithoutCurrentPrefix());
    }
    compared->state = visit(
        [&](auto&& state) -> decltype(Subtree::Compared::state) {
            using StateType = std::decay_t<decltype(state)>;
            if constexpr (std::is_same_v<StateType, Subtree::Compared::Unknown>) {
                return reconcileAgainstUnknownEncryption(schema, fieldPath);
            } else if constexpr (std::is_same_v<StateType, Subtree::Compared::NotEncrypted>) {
                return attemptReconcilingAgainstNoEncryption(
                    schema, fieldPath, compared->fields, compared->evaluated);
            } else if constexpr (std::is_same_v<StateType, Subtree::Compared::Encrypted>) {
                return attemptReconcilingAgainstEncryption(
                    schema, fieldPath, compared->fields, state.type);
            }
        },
        compared->state);
}

void attemptReconcilingFieldEncryption(const EncryptionSchemaTreeNode& schema,
                                       const ExpressionFieldPath& fieldPath,
                                       std::stack<Subtree>& subtreeStack) {
    visit(
        [&](auto&& output) {
            using OutputType = std::decay_t<decltype(output)>;
            // We don't keep records and everything is admissible if output is Forwarded.
            if constexpr (std::is_same_v<OutputType, Subtree::Forwarded>)
                ;
            // If output is Compared, we need to keep track of the fields referenced and potentially
            // throw an error.
            else if constexpr (std::is_same_v<OutputType, Subtree::Compared>)
                attemptReconcilingFieldEncryptionInCompared(schema, fieldPath, &output);
            // Evaluated output type requires to strictly check for errors, There is no need to keep
            // track of fields since the pressence of any encrypted fields is an immediate error.
            else if constexpr (std::is_same_v<OutputType, Subtree::Evaluated>)
                errorIfEncryptedFieldFoundInEvaluated(schema, fieldPath, &output);
        },
        subtreeStack.top().output);
}

/**
 * We must accomplish a few maintenance tasks here if we are in a Compared output Subtree:
 * * We must assert that we are in an Unknown or NotEncrypted state.
 * * We must transition to a NotEncrypted state.
 * * We must add the reasoning behind this call to the evaluated vector which provides explanations
 *   for error messages.
 */
void ensureNotEncrypted(const StringData reason, std::stack<Subtree>& subtreeStack) {
    if (auto compared = get_if<Subtree::Compared>(&subtreeStack.top().output)) {
        visit(
            [&](auto&& state) {
                using StateType = std::decay_t<decltype(state)>;
                if constexpr (!std::is_same_v<StateType, Subtree::Compared::Unknown> &&
                              !std::is_same_v<StateType, Subtree::Compared::NotEncrypted>) {
                    uassertedEvaluationInComparedEncryptedSubtree(reason, compared->fields);
                }
            },
            compared->state);
        compared->state = Subtree::Compared::NotEncrypted{};
        compared->evaluated.push_back(reason);
    }
}

/**
 * Here we attempt to reconcile against a variable access. All user-bound variables are unencrypted
 * since their definitions were walked in an Evaluated output type. All existing system variables
 * are also deemed unencrypted with the exception of CURRENT and ROOT which refer to the whole
 * document (CURRENT is not rebindable in FLE).
 */
void reconcileVariableAccess(const ExpressionFieldPath& variableFieldPath,
                             std::stack<Subtree>& subtreeStack) {
    visit(
        [&](auto&& output) {
            auto&& variableName = variableFieldPath.getFieldPath().getFieldName(0);
            using OutputType = std::decay_t<decltype(output)>;
            // Within Forwarded output Subtrees we have no concerns about what a variable could
            // refer to.
            if constexpr (std::is_same_v<OutputType, Subtree::Forwarded>)
                ;
            else if constexpr (std::is_same_v<OutputType, Subtree::Compared> ||
                               std::is_same_v<OutputType, Subtree::Evaluated>)
                // Forbid CURRENT and ROOT. They could be supported after support for Object
                // comparisons is added.
                if (variableName == "CURRENT" || variableName == "ROOT")
                    uassertedForbiddenVariable(variableName);
        },
        subtreeStack.top().output);
}

/**
 * Helper to determine if a comparison expression has exactly a field path and a constant. Returns
 * a pointer to each of the children of that type, or nullptrs if the types are not correct.
 */
std::pair<ExpressionFieldPath*, ExpressionConstant*> getFieldPathAndConstantFromExpression(
    ExpressionNary* expr) {

    tassert(6720804,
            "Expected expression with exactly two operands",
            expr->getOperandList().size() == 2);
    auto firstChild = expr->getOperandList()[0].get();
    auto firstOpFieldPath = dynamic_cast<ExpressionFieldPath*>(firstChild);
    auto secondChild = expr->getOperandList()[1].get();
    auto secondOpFieldPath = dynamic_cast<ExpressionFieldPath*>(secondChild);
    if (firstOpFieldPath) {
        return {firstOpFieldPath, dynamic_cast<ExpressionConstant*>(secondChild)};
    } else if (secondOpFieldPath) {
        return {secondOpFieldPath, dynamic_cast<ExpressionConstant*>(firstChild)};
    }
    return {nullptr, nullptr};
}

ExpressionFieldPath* getFieldpathForEncryptedCompare(ExpressionCompare* compare) {
    auto [relevantPath, relevantConstant] = getFieldPathAndConstantFromExpression(compare);
    if (!relevantPath || !relevantConstant) {
        return nullptr;
    }
    auto constVal = relevantConstant->getValue();
    if (isEncryptedPayload(constVal)) {
        return relevantPath;
    }
    return nullptr;
}

bool isEncryptedPayload(Value val) {
    return (val.getType() == BSONType::BinData) && (val.getBinData().type == BinDataType::Encrypt);
}
void IntentionPreVisitorBase::visit(ExpressionArray* array) {
    // Most of the time it is illegal to use an array in an encrypted context. For example it
    // would not make sense to allow {$eq: ["$ssn", [<anything>, <anything>]]}. However, there
    // are some exceptions to this rule such as in the second argument to an $in expression,
    // e.g.  {$in: ["$ssn", ["123-45-6789", "012-34-5678"]]}). To determine whether a literal is
    // allowed in the current context we must examine the Subtree stack and check if a
    // previously vistied expression determined it was ok.
    if (auto comparedSubtree = get_if<Subtree::Compared>(&subtreeStack.top().output);
        comparedSubtree && comparedSubtree->temporarilyPermittedArrayLiteral) {
        invariant(array == comparedSubtree->temporarilyPermittedArrayLiteral,
                  "Attempted to allow an array expression but visited a different array first");
        comparedSubtree->temporarilyPermittedArrayLiteral = nullptr;
        return;
    }
    ensureNotEncryptedEnterEval("formation of an array literal", subtreeStack);
}

void IntentionPreVisitorBase::visit(ExpressionFieldPath* fieldPath) {
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
        if (auto comparedSubtree = get_if<Subtree::Compared>(&subtreeStack.top().output);
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
        if (auto compared = get_if<Subtree::Compared>(&subtreeStack.top().output))
            compared->fields.push_back(fieldPath->getFieldPathWithoutCurrentPrefix());
    }
}

void IntentionPreVisitorBase::visit(ExpressionIn* in) {
    // Regardless of the below analysis, an $in expression is going to output an unencrypted
    // boolean. So if the result of this expression is being compared to encrypted values, it's
    // not going to work.
    ensureNotEncrypted("an $in expression", subtreeStack);

    // In most cases we can't work with arrays in this visitor, but $in is an interesting
    // exception.
    //     If the second argument to $in is an array literal, we know that the things inside
    // that array are going to be compared to the first argument and so by walking "through" the
    // ExpressionArray in a Compared Subtree we can correctly perform the encryption analysis.
    // We use a special flag on the Compared Subtree to communicate to the ExpressionArray that
    // it is allowed in this case. This state is set during 'inVisit()' to make sure we don't
    // change any analysis of the first child.
    //     If however the second argument is not an array literal then we must fail if it
    // contains anything encrypted. For example, if we have
    // {$in: ["xx-yyy-zzz", "$allowlistedSSNs"]} and 'allowlistedSSNs' is encrypted, we won't be
    // able to look within the array to evaluate the $in. So in these cases we add an
    // 'Evaluated' Subtree to make sure none of the arguments are encrypted.
    if (auto arrExpr = dynamic_cast<ExpressionArray*>(in->getOperandList()[1].get())) {
        auto comparedSubtree = Subtree::Compared{};
        // We also specifically support cases like: {$in: ["$encryptedField", <arr>]}. We set
        // temporarilyPermittedEncryptedFieldPath to the child field path to indicate that the
        // encrypted field path is allowed in this case.
        if (auto firstOp = dynamic_cast<ExpressionFieldPath*>(in->getOperandList()[0].get())) {
            auto ref = firstOp->getFieldPathWithoutCurrentPrefix().fullPath();
            if (schema.parsedFrom == FleVersion::kFle2 &&
                (schema.getEncryptionMetadataForPath(FieldRef{ref}) ||
                 schema.mayContainEncryptedNodeBelowPrefix(FieldRef{ref}))) {
                comparedSubtree.temporarilyPermittedEncryptedFieldPath = firstOp;
                for (auto& elem : arrExpr->getChildren()) {
                    ensureFLE2EncryptedFieldComparedToConstant(firstOp, elem.get());
                }
            }
        }
        enterSubtree(comparedSubtree, subtreeStack);
    } else {
        enterSubtree(Subtree::Evaluated{"an $in comparison without an array literal"},
                     subtreeStack);
    }
}


}  // namespace mongo::aggregate_expression_intender
