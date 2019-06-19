/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
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
#include "mongo/util/if_constexpr.h"
#include "query_analysis.h"
#include "resolved_encryption_info.h"

namespace mongo::aggregate_expression_intender {

namespace {

using namespace fmt::literals;
using namespace std::string_literals;

/**
 * A struct representing a collection of Expression nodes in a tree. Note that each node belongs to
 * at most one Subtree. This means that the children of any given Expression might belong to the
 * same Subtree as their parent or be the start of a new Subtree.
 */
struct Subtree {

    // The following structs are output types. Each Subtree will be assigned one of them when it is
    // created. Depending on the output type of the current Subtree we will change the logic of the
    // analysis.

    /**
     * This permissive output type indicates a place in the input where data is returned to the user
     * rather than processed by the server. Encrypted and Unencrypted values are allowed in any
     * combination in such a Subtree so we don't bother keeping track of the state. Note that
     * descendant Subtrees may be scrutinized under a different output type.
     */
    struct Forwarded {};
    /**
     * This output type indicates the children of a comparison. If any of these are field
     * references, they must either all be unencrypted or share the same encryption type. Entering a
     * mixed state is banned under a Compared Subtree.
     */
    struct Compared {
        /**
         * Storing the fields we've seen so far in the Subtree lets us write better error messages.
         * For example to produce the message:
         *     "...encryption algorithm for field a does not match the algorithm of b c"
         * This vector will contain the FieldPaths 'b' and 'c'.
         **/
        std::vector<FieldPath> fields;
        /**
         * We also store strings naming any evaluated values we've seen feeding into this Subtree.
         */
        std::vector<StringData> evaluated;
        /**
         * Here we store pointers to any literals we've encountered. If a Subtree with the Compared
         * output type is exited, we will replace each literal with an intent-to-encrypt marking if
         * the Subtree we exited was in the Encrypted state.
         */
        std::vector<ExpressionConstant*> literals;

        // Whether or not an array literal is allowed in the expression tree is unfortunately
        // context-specific. We use this member variable to track whether or not we are in a special
        // circumstance where we can allow an array literal. For example, this is set when visiting
        // the second argument of an $in expression. Because this is so specific, we use a pointer
        // to the specific ExpressionArray we know is allowed to add an extra layer of defense
        // against accidentally allowing arrays where we did not intend to do so.
        ExpressionArray* temporarilyPermittedArrayLiteral{nullptr};

        // The following structs are state types. Each Compared Subtree starts in the Unkown state
        // and transitions into the NotEncrypted or Encrypted state.

        /**
         * The presence of an object of this type indicates we have yet to determine the encryption
         * status for this Subtree. It is valid for a Subtree to remain Unknown even after a full
         * walk, this just means it contains no field references.
         */
        struct Unknown {};
        /**
         * A Subtree with this struct present contains only field references without encryption.
         */
        struct NotEncrypted {};
        /**
         * A Subtree with this struct present contains field references with encryption. The
         * encryption type is available through the included member.
         */
        struct Encrypted {
            ResolvedEncryptionInfo type;
        };

        stdx::variant<Unknown, NotEncrypted, Encrypted> state;
    };
    /**
     * This output type indicates a value that is read by the server for some purpose other than
     * determining equality, for example $multiply. Entering any state besides NotEncrypted is
     * forbidden under a Subtree with this output type. Therefore we do not keep track of the state
     * and simply throw an error when we see an encrypted field referenced. Included is a string
     * that explains what's doing the evaluation for the purpose of improving error messages.
     */
    struct Evaluated {
        const StringData by;
    };

    stdx::variant<Forwarded, Compared, Evaluated> output;
};

std::string toString(const decltype(Subtree::output)& outputType) {
    return stdx::visit(
        [&](auto&& outputType) {
            using OutputType = std::decay_t<decltype(outputType)>;
            IF_CONSTEXPR(std::is_same_v<OutputType, Subtree::Forwarded>) {
                return "Subtree::Forwarded";
            }
            else if constexpr(std::is_same_v<OutputType, Subtree::Compared>) {
                return "Subtree::Compared";
            }
            else if constexpr(std::is_same_v<OutputType, Subtree::Evaluated>) {
                return "Subtree::Evaluated";
            }
        },
        outputType);
}

template <typename T>
std::string toString() {
    IF_CONSTEXPR(std::is_same_v<T, Subtree::Forwarded>) {
        return "Subtree::Forwarded";
    }
    else if constexpr(std::is_same_v<T, Subtree::Evaluated>) {
        return "Subtree::Evaluated";
    }
    else if constexpr(std::is_same_v<T, Subtree::Compared>) {
        return "Subtree::Compared";
    }
}

void rewriteLiteralToIntent(const ExpressionContext& expCtx,
                            const ResolvedEncryptionInfo& encryptedType,
                            ExpressionConstant* literal) {
    using namespace cryptd_query_analysis;
    literal->setValue(buildEncryptPlaceholder(literal->getValue(),
                                              encryptedType,
                                              EncryptionPlaceholderContext::kComparison,
                                              expCtx.getCollator()));
}

void enterSubtree(decltype(Subtree::output) outputType, std::stack<Subtree>& subtreeStack) {
    subtreeStack.push({outputType});
}

template <typename Out>
Intention exitSubtree(const ExpressionContext& expCtx, std::stack<Subtree>& subtreeStack) {
    bool literalRewritten = false;
    if (auto compared = stdx::get_if<Subtree::Compared>(&subtreeStack.top().output))
        if (auto encrypted = stdx::get_if<Subtree::Compared::Encrypted>(&compared->state)) {
            for (auto&& literal : compared->literals)
                rewriteLiteralToIntent(expCtx, encrypted->type, literal);
            literalRewritten = compared->literals.size() > 0;
        }

    // It's really easy to push and forget to pop (enter but not exit). As a layer of safety we
    // verify that you are popping off the stack the type you expect to be popping.
    stdx::visit(
        [](auto&& output) {
            using OutputType = std::decay_t<decltype(output)>;
            IF_CONSTEXPR(!std::is_same_v<OutputType, Out>) {
                // Due to a bug in gcc we can't inline 'msg' into the invariant statement below:
                // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86969
                // This is a workaround, once we upgrade to a version of gcc which has a fix for
                // that bug (e.g. gcc 9.1), we can move 'msg' inline.
                std::string msg =
                    "exiting a subtree of an unexpected type. Expected {}, found {}"_format(
                        toString<Out>(), toString(output));
                invariant(false, msg);
            }

        },
        subtreeStack.top().output);

    subtreeStack.pop();

    return literalRewritten ? Intention::Marked : Intention::NotMarked;
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

[[noreturn]] void uassertedForbiddenVariable(const StringData& variableName) {
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
            encryptedType || !schema.containsEncryptedNodeBelowPrefix(FieldRef(path.fullPath())));
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

void errorIfEncryptedFieldFoundInEvaluated(const EncryptionSchemaTreeNode& schema,
                                           const ExpressionFieldPath& fieldPath,
                                           Subtree::Evaluated* evaluated) {
    if (getEncryptionTypeForPathEnsureNotPrefix(schema, fieldPath))
        // The examined field is encrypted and the output type of the current Subtree disallows any
        // encrypted fields.
        uassertedEncryptedInEvaluatedContext(fieldPath.getFieldPathWithoutCurrentPrefix(),
                                             evaluated->by);
}

void attemptReconcilingFieldEncryptionInCompared(const EncryptionSchemaTreeNode& schema,
                                                 const ExpressionFieldPath& fieldPath,
                                                 Subtree::Compared* compared) {
    // Any reference to a randomly encrypted field within a comparison subtree will fail.
    auto metadata = schema.getEncryptionMetadataForPath(
        FieldRef(fieldPath.getFieldPathWithoutCurrentPrefix().fullPath()));
    if (metadata && metadata->algorithm == FleAlgorithmEnum::kRandom) {
        uassertedComparisonOfRandomlyEncrypted(fieldPath.getFieldPathWithoutCurrentPrefix());
    }
    compared->state = stdx::visit(
        [&](auto&& state) -> decltype(Subtree::Compared::state) {
            using StateType = std::decay_t<decltype(state)>;
            IF_CONSTEXPR(std::is_same_v<StateType, Subtree::Compared::Unknown>) {
                return reconcileAgainstUnknownEncryption(schema, fieldPath);
            }
            else if constexpr(std::is_same_v<StateType, Subtree::Compared::NotEncrypted>) {
                return attemptReconcilingAgainstNoEncryption(
                    schema, fieldPath, compared->fields, compared->evaluated);
            }
            else if constexpr(std::is_same_v<StateType, Subtree::Compared::Encrypted>) {
                return attemptReconcilingAgainstEncryption(
                    schema, fieldPath, compared->fields, state.type);
            }
        },
        compared->state);
}

void attemptReconcilingFieldEncryption(const EncryptionSchemaTreeNode& schema,
                                       const ExpressionFieldPath& fieldPath,
                                       std::stack<Subtree>& subtreeStack) {
    stdx::visit(
        [&](auto&& output) {
            using OutputType = std::decay_t<decltype(output)>;
            // We don't keep records and everything is admissible if output is Forwarded.
            IF_CONSTEXPR(std::is_same_v<OutputType, Subtree::Forwarded>);
            // If output is Compared, we need to keep track of the fields referenced and potentially
            // throw an error.
            else if constexpr(std::is_same_v<OutputType, Subtree::Compared>)
                attemptReconcilingFieldEncryptionInCompared(schema, fieldPath, &output);
            // Evaluated output type requires to strictly check for errors, There is no need to keep
            // track of fields since the pressence of any encrypted fields is an immediate error.
            else if constexpr(std::is_same_v<OutputType, Subtree::Evaluated>)
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
    if (auto compared = stdx::get_if<Subtree::Compared>(&subtreeStack.top().output)) {
        stdx::visit(
            [&](auto&& state) {
                using StateType = std::decay_t<decltype(state)>;
                IF_CONSTEXPR(!std::is_same_v<StateType, Subtree::Compared::Unknown> &&
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
 * This calls ensureNotEncrypted while providing the upcoming Evaluated Subtree's by string as the
 * reasoning behind the assertion that this Subtree is NotEncrypted or not Compared. Regardless of
 * our current Subtree output type, we then enter a new Evaluated Subtree.
 */
void ensureNotEncryptedEnterEval(const StringData evaluation, std::stack<Subtree>& subtreeStack) {
    ensureNotEncrypted(evaluation, subtreeStack);
    enterSubtree(Subtree::Evaluated{evaluation}, subtreeStack);
}

/**
 * Here we attempt to reconcile against a variable access. All user-bound variables are unencrypted
 * since their definitions were walked in an Evaluated output type. All existing system variables
 * are also deemed unencrypted with the exception of CURRENT and ROOT which refer to the whole
 * document (CURRENT is not rebindable in FLE).
 */
void reconcileVariableAccess(const ExpressionFieldPath& variableFieldPath,
                             std::stack<Subtree>& subtreeStack) {
    stdx::visit(
        [&](auto&& output) {
            auto&& variableName = variableFieldPath.getFieldPath().getFieldName(0);
            using OutputType = std::decay_t<decltype(output)>;
            // Within Forwarded output Subtrees we have no concerns about what a variable could
            // refer to.
            if
                constexpr(std::is_same_v<OutputType, Subtree::Forwarded>);
            else if
                constexpr(std::is_same_v<OutputType, Subtree::Compared> ||
                          std::is_same_v<OutputType, Subtree::Evaluated>)
                    // Forbid CURRENT and ROOT. They could be supported after support for Object
                    // comparisons is added.
                    if (variableName == "CURRENT" || variableName == "ROOT")
                        uassertedForbiddenVariable(variableName);
        },
        subtreeStack.top().output);
}

/**
 * We prefer front-loading work and doing as much as possible in the PreVisitor for
 * organization.
 */
class IntentionPreVisitor final : public ExpressionVisitor {
public:
    IntentionPreVisitor(const ExpressionContext& expCtx,
                        const EncryptionSchemaTreeNode& schema,
                        std::stack<Subtree>& subtreeStack)
        : expCtx(expCtx), schema(schema), subtreeStack(subtreeStack) {}

private:
    void visit(ExpressionConstant* constant) final {
        if (auto compared = stdx::get_if<Subtree::Compared>(&subtreeStack.top().output))
            compared->literals.push_back(constant);
    }
    void visit(ExpressionAbs*) final {
        ensureNotEncryptedEnterEval("an absolute value calculation", subtreeStack);
    }
    void visit(ExpressionAdd*) final {
        ensureNotEncryptedEnterEval("an addition calculation", subtreeStack);
    }
    void visit(ExpressionAllElementsTrue*) final {
        ensureNotEncryptedEnterEval("an 'all elements true' expression", subtreeStack);
    }
    void visit(ExpressionAnd*) final {
        ensureNotEncryptedEnterEval("a conjunction", subtreeStack);
    }
    void visit(ExpressionAnyElementTrue*) final {
        ensureNotEncryptedEnterEval("an 'any elements true' expression", subtreeStack);
    }
    void visit(ExpressionArray* array) final {
        // Most of the time it is illegal to use an array in an encrypted context. For example it
        // would not make sense to allow {$eq: ["$ssn", [<anything>, <anything>]]}. However, there
        // are some exceptions to this rule such as in the second argument to an $in expression,
        // e.g.  {$in: ["$ssn", ["123-45-6789", "012-34-5678"]]}). To determine whether a literal is
        // allowed in the current context we must examine the Subtree stack and check if a
        // previously vistied expression determined it was ok.
        if (auto comparedSubtree = stdx::get_if<Subtree::Compared>(&subtreeStack.top().output);
            comparedSubtree && comparedSubtree->temporarilyPermittedArrayLiteral) {
            invariant(array == comparedSubtree->temporarilyPermittedArrayLiteral,
                      "Attempted to allow an array expression but visited a different array first");
            comparedSubtree->temporarilyPermittedArrayLiteral = nullptr;
            return;
        }
        ensureNotEncryptedEnterEval("formation of an array literal", subtreeStack);
    }
    void visit(ExpressionArrayElemAt*) final {
        ensureNotEncryptedEnterEval("array indexing", subtreeStack);
    }
    void visit(ExpressionObjectToArray*) final {
        ensureNotEncryptedEnterEval("an object to array conversion", subtreeStack);
    }
    void visit(ExpressionArrayToObject*) final {
        ensureNotEncryptedEnterEval("an array to object conversion", subtreeStack);
    }
    void visit(ExpressionCeil*) final {
        ensureNotEncryptedEnterEval("a ceiling calculation", subtreeStack);
    }
    void visit(ExpressionCoerceToBool*) final {
        ensureNotEncryptedEnterEval("a coercion to boolean", subtreeStack);
    }
    void visit(ExpressionCompare* compare) final {
        switch (compare->getOp()) {
            case ExpressionCompare::EQ:
            case ExpressionCompare::NE:
                // The result of this comparison will be either true or false, never encrypted. So
                // if the Subtree above us is comparing to an encrypted value that has to be an
                // error.
                ensureNotEncrypted("an equality comparison", subtreeStack);
                // Now that we're sure our result won't be compared to encrypted values, enter a new
                // Subtree to provide a new context for our children - this is a fresh start.
                enterSubtree(Subtree::Compared{}, subtreeStack);
                return;
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
    void visit(ExpressionConcat*) final {
        ensureNotEncryptedEnterEval("string concatination", subtreeStack);
    }
    void visit(ExpressionConcatArrays*) final {
        ensureNotEncryptedEnterEval("array concatination", subtreeStack);
    }
    void visit(ExpressionCond*) final {
        // We need to enter an Evaluated Subtree for the first child of the $cond (if).
        enterSubtree(Subtree::Evaluated{"a boolean conditional"}, subtreeStack);
    }
    void visit(ExpressionDateFromString*) final {
        ensureNotEncryptedEnterEval("date from string function", subtreeStack);
    }
    void visit(ExpressionDateFromParts*) final {
        ensureNotEncryptedEnterEval("date from parts function", subtreeStack);
    }
    void visit(ExpressionDateToParts*) final {
        ensureNotEncryptedEnterEval("date to parts function", subtreeStack);
    }
    void visit(ExpressionDateToString*) final {
        ensureNotEncryptedEnterEval("date to string function", subtreeStack);
    }
    void visit(ExpressionDivide*) final {
        ensureNotEncryptedEnterEval("division", subtreeStack);
    }
    void visit(ExpressionExp*) final {
        ensureNotEncryptedEnterEval("an exponentiation", subtreeStack);
    }
    void visit(ExpressionFieldPath* fieldPath) final {
        // Variables are handled with seperate logic from field references.
        if (fieldPath->getFieldPath().getFieldName(0) != "CURRENT" ||
            fieldPath->getFieldPath().getPathLength() <= 1) {
            reconcileVariableAccess(*fieldPath, subtreeStack);
        } else {
            attemptReconcilingFieldEncryption(schema, *fieldPath, subtreeStack);
            // Indicate that we've seen this field to improve error messages if we see an
            // incompatible field later.
            if (auto compared = stdx::get_if<Subtree::Compared>(&subtreeStack.top().output))
                compared->fields.push_back(fieldPath->getFieldPathWithoutCurrentPrefix());
        }
    }
    void visit(ExpressionFilter*) final {
        ensureNotEncryptedEnterEval("an array filter", subtreeStack);
    }
    void visit(ExpressionFloor*) final {
        ensureNotEncryptedEnterEval("a floor calculation", subtreeStack);
    }
    void visit(ExpressionIfNull*) final {
        // If $ifNull appears under a comparison subtree, then both arguments to $ifNull should be
        // marked or assert just as if they were the direct descendant of the grandparent
        // comparison.
    }
    void visit(ExpressionIn* in) final {
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
        // {$in: ["xx-yyy-zzz", "$whitelistedSSNs"]} and 'whitelistedSSNs' is encrypted, we won't be
        // able to look within the array to evaluate the $in. So in these cases we add an
        // 'Evaluated' Subtree to make sure none of the arguments are encrypted.
        if (dynamic_cast<ExpressionArray*>(in->getOperandList()[1].get())) {
            enterSubtree(Subtree::Compared{}, subtreeStack);
        } else {
            enterSubtree(Subtree::Evaluated{"an $in comparison without an array literal"},
                         subtreeStack);
        }
    }
    void visit(ExpressionIndexOfArray*) final {
        ensureNotEncryptedEnterEval("an array find", subtreeStack);
    }
    void visit(ExpressionIndexOfBytes*) final {
        ensureNotEncryptedEnterEval("a byte-based string find", subtreeStack);
    }
    void visit(ExpressionIndexOfCP*) final {
        ensureNotEncryptedEnterEval("a code-point-based string find", subtreeStack);
    }
    void visit(ExpressionLet* let) final {
        for (auto && [ unused, nameAndExpression ] : let->getVariableMap())
            if (auto && [ name, unused ] = nameAndExpression; name == "CURRENT")
                uasserted(31152, "Rebinding of CURRENT disallowed");
        // It's possible for a $let to have no bindings, so entering a Subtree is conditional on
        // having at least one.
        if (let->getChildren().size() > 1)
            enterSubtree(Subtree::Evaluated{"a let binding"}, subtreeStack);
    }
    void visit(ExpressionLn*) final {
        ensureNotEncryptedEnterEval("a natural logarithm calculation", subtreeStack);
    }
    void visit(ExpressionLog*) final {
        ensureNotEncryptedEnterEval("a logarithm calculation", subtreeStack);
    }
    void visit(ExpressionLog10*) final {
        ensureNotEncryptedEnterEval("a base-ten logarithm calculation", subtreeStack);
    }
    void visit(ExpressionMap*) final {
        ensureNotEncryptedEnterEval("a map function", subtreeStack);
    }
    void visit(ExpressionMeta*) final {
        ensureNotEncrypted("a metadata access", subtreeStack);
    }
    void visit(ExpressionMod*) final {
        ensureNotEncryptedEnterEval("a modulo calculation", subtreeStack);
    }
    void visit(ExpressionMultiply*) final {
        ensureNotEncryptedEnterEval("a multiplication calculation", subtreeStack);
    }
    void visit(ExpressionNot*) final {
        ensureNotEncryptedEnterEval("a negation", subtreeStack);
    }
    void visit(ExpressionObject*) final {
        // Arguably this isn't evaluation but it has the same semantics for now. We could support
        // this with effort.
        ensureNotEncryptedEnterEval("formation of an object literal", subtreeStack);
    }
    void visit(ExpressionOr*) final {
        ensureNotEncryptedEnterEval("a disjunction", subtreeStack);
    }
    void visit(ExpressionPow*) final {
        ensureNotEncryptedEnterEval("an exponentiation calculation", subtreeStack);
    }
    void visit(ExpressionRange*) final {
        ensureNotEncryptedEnterEval("a numeric sequence generator", subtreeStack);
    }
    void visit(ExpressionReduce*) final {
        enterSubtree(Subtree::Evaluated{"a reduce initializer"}, subtreeStack);
    }
    void visit(ExpressionSetDifference*) final {
        ensureNotEncryptedEnterEval("a set difference operation", subtreeStack);
    }
    void visit(ExpressionSetEquals*) final {
        ensureNotEncryptedEnterEval("a set equality operation", subtreeStack);
    }
    void visit(ExpressionSetIntersection*) final {
        ensureNotEncryptedEnterEval("a set intersection operation", subtreeStack);
    }
    void visit(ExpressionSetIsSubset*) final {
        ensureNotEncryptedEnterEval("a subset determination operation", subtreeStack);
    }
    void visit(ExpressionSetUnion*) final {
        ensureNotEncryptedEnterEval("a set union operation", subtreeStack);
    }
    void visit(ExpressionSize*) final {
        ensureNotEncryptedEnterEval("an array size determination", subtreeStack);
    }
    void visit(ExpressionReverseArray*) final {
        ensureNotEncryptedEnterEval("an array reversal", subtreeStack);
    }
    void visit(ExpressionSlice*) final {
        ensureNotEncryptedEnterEval("an array subset operation", subtreeStack);
    }
    void visit(ExpressionIsArray*) final {
        ensureNotEncryptedEnterEval("an array type determination", subtreeStack);
    }
    void visit(ExpressionRound*) final {
        ensureNotEncryptedEnterEval("a rounding calculation", subtreeStack);
    }
    void visit(ExpressionSplit*) final {
        ensureNotEncryptedEnterEval("a string split", subtreeStack);
    }
    void visit(ExpressionSqrt*) final {
        ensureNotEncryptedEnterEval("a square root calculation", subtreeStack);
    }
    void visit(ExpressionStrcasecmp*) final {
        ensureNotEncryptedEnterEval("a case-insensitive string comparison", subtreeStack);
    }
    void visit(ExpressionSubstrBytes*) final {
        ensureNotEncryptedEnterEval("a byte-based substring operation", subtreeStack);
    }
    void visit(ExpressionSubstrCP*) final {
        ensureNotEncryptedEnterEval("a code-point-based substring operation", subtreeStack);
    }
    void visit(ExpressionStrLenBytes*) final {
        ensureNotEncryptedEnterEval("a byte-based string length determination", subtreeStack);
    }
    void visit(ExpressionStrLenCP*) final {
        ensureNotEncryptedEnterEval("a code-point-based string length determination", subtreeStack);
    }
    void visit(ExpressionSubtract*) final {
        ensureNotEncryptedEnterEval("a subtraction calculation", subtreeStack);
    }
    void visit(ExpressionSwitch*) final {
        // We need to enter an Evaluated output Subtree for each case child.
        enterSubtree(Subtree::Evaluated{"a switch case"}, subtreeStack);
    }
    void visit(ExpressionToLower*) final {
        ensureNotEncryptedEnterEval("a string lowercase conversion", subtreeStack);
    }
    void visit(ExpressionToUpper*) final {
        ensureNotEncryptedEnterEval("a string uppercase conversion", subtreeStack);
    }
    void visit(ExpressionTrim*) final {
        ensureNotEncryptedEnterEval("a string trim operation", subtreeStack);
    }
    void visit(ExpressionTrunc*) final {
        ensureNotEncryptedEnterEval("a string truncation operation", subtreeStack);
    }
    void visit(ExpressionType*) final {
        ensureNotEncryptedEnterEval("a string type determination", subtreeStack);
    }
    void visit(ExpressionZip*) final {
        ensureNotEncryptedEnterEval("an array zip operation", subtreeStack);
    }
    void visit(ExpressionConvert*) final {
        ensureNotEncryptedEnterEval("a type conversion", subtreeStack);
    }
    void visit(ExpressionRegexFind*) final {
        ensureNotEncryptedEnterEval("a regex find operation", subtreeStack);
    }
    void visit(ExpressionRegexFindAll*) final {
        ensureNotEncryptedEnterEval("a regex find all operation", subtreeStack);
    }
    void visit(ExpressionRegexMatch*) final {
        ensureNotEncryptedEnterEval("a regex match operation", subtreeStack);
    }
    void visit(ExpressionCosine*) final {
        ensureNotEncryptedEnterEval("a cosine calculation", subtreeStack);
    }
    void visit(ExpressionSine*) final {
        ensureNotEncryptedEnterEval("a sine calculation", subtreeStack);
    }
    void visit(ExpressionTangent*) final {
        ensureNotEncryptedEnterEval("a tangent calculation", subtreeStack);
    }
    void visit(ExpressionArcCosine*) final {
        ensureNotEncryptedEnterEval("an inverse cosine calculation", subtreeStack);
    }
    void visit(ExpressionArcSine*) final {
        ensureNotEncryptedEnterEval("an inverse sine calculation", subtreeStack);
    }
    void visit(ExpressionArcTangent*) final {
        ensureNotEncryptedEnterEval("an inverse tangent calculation", subtreeStack);
    }
    void visit(ExpressionArcTangent2*) final {
        ensureNotEncryptedEnterEval("an inverse tangent calculation", subtreeStack);
    }
    void visit(ExpressionHyperbolicArcTangent*) final {
        ensureNotEncryptedEnterEval("a hyperbolic inverse tangent calculation", subtreeStack);
    }
    void visit(ExpressionHyperbolicArcCosine*) final {
        ensureNotEncryptedEnterEval("a hyperbolic inverse cosine calculation", subtreeStack);
    }
    void visit(ExpressionHyperbolicArcSine*) final {
        ensureNotEncryptedEnterEval("a hyperbolic inverse sine calculation", subtreeStack);
    }
    void visit(ExpressionHyperbolicTangent*) final {
        ensureNotEncryptedEnterEval("a hyperbolic tangent calculation", subtreeStack);
    }
    void visit(ExpressionHyperbolicCosine*) final {
        ensureNotEncryptedEnterEval("a hyperbolic cosine calculation", subtreeStack);
    }
    void visit(ExpressionHyperbolicSine*) final {
        ensureNotEncryptedEnterEval("a hyperbolic sine calculation", subtreeStack);
    }
    void visit(ExpressionDegreesToRadians*) final {
        ensureNotEncryptedEnterEval("a degree to radian conversion", subtreeStack);
    }
    void visit(ExpressionRadiansToDegrees*) final {
        ensureNotEncryptedEnterEval("a radian to degree conversion", subtreeStack);
    }
    void visit(ExpressionDayOfMonth*) final {
        ensureNotEncryptedEnterEval("a day of month extractor", subtreeStack);
    }
    void visit(ExpressionDayOfWeek*) final {
        ensureNotEncryptedEnterEval("a day of week extractor", subtreeStack);
    }
    void visit(ExpressionDayOfYear*) final {
        ensureNotEncryptedEnterEval("a day of year extractor", subtreeStack);
    }
    void visit(ExpressionHour*) final {
        ensureNotEncryptedEnterEval("an hour extractor", subtreeStack);
    }
    void visit(ExpressionMillisecond*) final {
        ensureNotEncryptedEnterEval("a millisecond extractor", subtreeStack);
    }
    void visit(ExpressionMinute*) final {
        ensureNotEncryptedEnterEval("a minute extractor", subtreeStack);
    }
    void visit(ExpressionMonth*) final {
        ensureNotEncryptedEnterEval("a month extractor", subtreeStack);
    }
    void visit(ExpressionSecond*) final {
        ensureNotEncryptedEnterEval("a second extractor", subtreeStack);
    }
    void visit(ExpressionWeek*) final {
        ensureNotEncryptedEnterEval("a week of year extractor", subtreeStack);
    }
    void visit(ExpressionIsoWeekYear*) final {
        ensureNotEncryptedEnterEval("an ISO 8601 week and year extractor", subtreeStack);
    }
    void visit(ExpressionIsoDayOfWeek*) final {
        ensureNotEncryptedEnterEval("an ISO 8601 day of week extractor", subtreeStack);
    }
    void visit(ExpressionIsoWeek*) final {
        ensureNotEncryptedEnterEval("an ISO 8601 week extractor", subtreeStack);
    }
    void visit(ExpressionYear*) final {
        ensureNotEncryptedEnterEval("an ISO 8601 year extractor", subtreeStack);
    }
    void visit(ExpressionFromAccumulator<AccumulatorAvg>*) final {
        ensureNotEncryptedEnterEval("an average aggregation", subtreeStack);
    }
    void visit(ExpressionFromAccumulator<AccumulatorMax>*) final {
        ensureNotEncryptedEnterEval("a maximum aggregation", subtreeStack);
    }
    void visit(ExpressionFromAccumulator<AccumulatorMin>*) final {
        ensureNotEncryptedEnterEval("a minimum aggregation", subtreeStack);
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>*) final {
        ensureNotEncryptedEnterEval("a population standard deviation aggregation", subtreeStack);
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>*) final {
        ensureNotEncryptedEnterEval("a sample standard deviation aggregation", subtreeStack);
    }
    void visit(ExpressionFromAccumulator<AccumulatorSum>*) final {
        ensureNotEncryptedEnterEval("a sum aggregation", subtreeStack);
    }
    void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>*) final {
        ensureNotEncryptedEnterEval("a merge objects aggregation", subtreeStack);
    }
    void visit(ExpressionTests::Testable*) final {}

    const ExpressionContext& expCtx;
    const EncryptionSchemaTreeNode& schema;
    std::stack<Subtree>& subtreeStack;
};

class IntentionInVisitor final : public ExpressionVisitor {
public:
    IntentionInVisitor(const ExpressionContext& expCtx,
                       const EncryptionSchemaTreeNode& schema,
                       std::stack<Subtree>& subtreeStack)
        : expCtx(expCtx), schema(schema), subtreeStack(subtreeStack) {}

private:
    void visit(ExpressionConstant*) final {}
    void visit(ExpressionAbs*) final {}
    void visit(ExpressionAdd*) final {}
    void visit(ExpressionAllElementsTrue*) final {}
    void visit(ExpressionAnd*) final {}
    void visit(ExpressionAnyElementTrue*) final {}
    void visit(ExpressionArray*) final {}
    void visit(ExpressionArrayElemAt*) final {}
    void visit(ExpressionObjectToArray*) final {}
    void visit(ExpressionArrayToObject*) final {}
    void visit(ExpressionCeil*) final {}
    void visit(ExpressionCoerceToBool*) final {}
    void visit(ExpressionCompare*) final {}
    void visit(ExpressionConcat*) final {}
    void visit(ExpressionConcatArrays*) final {}
    void visit(ExpressionCond*) final {
        if (numChildrenVisited == 1ull)
            // We need to exit the Evaluated output Subtree for if child.
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
        // The then and else children should be part of the parent Subtree.
    }
    void visit(ExpressionDateFromString*) final {}
    void visit(ExpressionDateFromParts*) final {}
    void visit(ExpressionDateToParts*) final {}
    void visit(ExpressionDateToString*) final {}
    void visit(ExpressionDivide*) final {}
    void visit(ExpressionExp*) final {}
    void visit(ExpressionFieldPath*) final {}
    void visit(ExpressionFilter*) final {}
    void visit(ExpressionFloor*) final {}
    void visit(ExpressionIfNull*) final {}
    void visit(ExpressionIn* in) final {
        if (auto arrayLiteral = dynamic_cast<ExpressionArray*>(in->getOperandList()[1].get())) {
            // There must be a subtree with Compared output type at the top since we just put it
            // there.
            auto comparedSubtree = stdx::get_if<Subtree::Compared>(&subtreeStack.top().output);
            invariant(comparedSubtree,
                      "$in expected to find the Subtree::Compared that it pushed onto the stack. "
                      "Perhaps a subtree forgot to pop off the stack before exiting postVisit()?");
            comparedSubtree->temporarilyPermittedArrayLiteral = arrayLiteral;
        }
    }
    void visit(ExpressionIndexOfArray*) final {}
    void visit(ExpressionIndexOfBytes*) final {}
    void visit(ExpressionIndexOfCP*) final {}
    void visit(ExpressionLet* let) final {
        // The final child of a let Expression is part of the parent Subtree.
        if (numChildrenVisited == let->getChildren().size() - 1)
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionLn*) final {}
    void visit(ExpressionLog*) final {}
    void visit(ExpressionLog10*) final {}
    void visit(ExpressionMap*) final {}
    void visit(ExpressionMeta*) final {}
    void visit(ExpressionMod*) final {}
    void visit(ExpressionMultiply*) final {}
    void visit(ExpressionNot*) final {}
    void visit(ExpressionObject*) final {}
    void visit(ExpressionOr*) final {}
    void visit(ExpressionPow*) final {}
    void visit(ExpressionRange*) final {}
    void visit(ExpressionReduce* reduce) final {
        // As with ExpressionLet the final child here is part of the parent Subtree.
        if (numChildrenVisited == reduce->getChildren().size() - 1)
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSetDifference*) final {}
    void visit(ExpressionSetEquals*) final {}
    void visit(ExpressionSetIntersection*) final {}
    void visit(ExpressionSetIsSubset*) final {}
    void visit(ExpressionSetUnion*) final {}
    void visit(ExpressionSize*) final {}
    void visit(ExpressionReverseArray*) final {}
    void visit(ExpressionSlice*) final {}
    void visit(ExpressionIsArray*) final {}
    void visit(ExpressionRound*) final {}
    void visit(ExpressionSplit*) final {}
    void visit(ExpressionSqrt*) final {}
    void visit(ExpressionStrcasecmp*) final {}
    void visit(ExpressionSubstrBytes*) final {}
    void visit(ExpressionSubstrCP*) final {}
    void visit(ExpressionStrLenBytes*) final {}
    void visit(ExpressionStrLenCP*) final {}
    void visit(ExpressionSubtract*) final {}
    void visit(ExpressionSwitch* switchExpr) final {
        // The outer if skips the final (mandatory) default node.
        if (numChildrenVisited != switchExpr->getChildren().size() - 1) {
            // The first branch will be taken for each 'case' child. The second for each 'then'
            // child.
            if (numChildrenVisited % 2ull == 0ull)
                // We need to enter an Evaluated output Subtree for each case child.
                enterSubtree(Subtree::Evaluated{"a switch case"}, subtreeStack);
            else
                // After every odd child we need to exit the above Subtree.
                didSetIntention =
                    exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
        }
    }
    void visit(ExpressionToLower*) final {}
    void visit(ExpressionToUpper*) final {}
    void visit(ExpressionTrim*) final {}
    void visit(ExpressionTrunc*) final {}
    void visit(ExpressionType*) final {}
    void visit(ExpressionZip*) final {}
    void visit(ExpressionConvert*) final {}
    void visit(ExpressionRegexFind*) final {}
    void visit(ExpressionRegexFindAll*) final {}
    void visit(ExpressionRegexMatch*) final {}
    void visit(ExpressionCosine*) final {}
    void visit(ExpressionSine*) final {}
    void visit(ExpressionTangent*) final {}
    void visit(ExpressionArcCosine*) final {}
    void visit(ExpressionArcSine*) final {}
    void visit(ExpressionArcTangent*) final {}
    void visit(ExpressionArcTangent2*) final {}
    void visit(ExpressionHyperbolicArcTangent*) final {}
    void visit(ExpressionHyperbolicArcCosine*) final {}
    void visit(ExpressionHyperbolicArcSine*) final {}
    void visit(ExpressionHyperbolicTangent*) final {}
    void visit(ExpressionHyperbolicCosine*) final {}
    void visit(ExpressionHyperbolicSine*) final {}
    void visit(ExpressionDegreesToRadians*) final {}
    void visit(ExpressionRadiansToDegrees*) final {}
    void visit(ExpressionDayOfMonth*) final {}
    void visit(ExpressionDayOfWeek*) final {}
    void visit(ExpressionDayOfYear*) final {}
    void visit(ExpressionHour*) final {}
    void visit(ExpressionMillisecond*) final {}
    void visit(ExpressionMinute*) final {}
    void visit(ExpressionMonth*) final {}
    void visit(ExpressionSecond*) final {}
    void visit(ExpressionWeek*) final {}
    void visit(ExpressionIsoWeekYear*) final {}
    void visit(ExpressionIsoDayOfWeek*) final {}
    void visit(ExpressionIsoWeek*) final {}
    void visit(ExpressionYear*) final {}
    void visit(ExpressionFromAccumulator<AccumulatorAvg>*) final {}
    void visit(ExpressionFromAccumulator<AccumulatorMax>*) final {}
    void visit(ExpressionFromAccumulator<AccumulatorMin>*) final {}
    void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>*) final {}
    void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>*) final {}
    void visit(ExpressionFromAccumulator<AccumulatorSum>*) final {}
    void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>*) final {}
    void visit(ExpressionTests::Testable*) final {}

public:
    /**
     * The number of child nodes which have already been visited for a given parent node. This is
     * set by the Walker before inVisit is called.
     */
    unsigned long long numChildrenVisited;

    Intention didSetIntention = Intention::NotMarked;

private:
    const ExpressionContext& expCtx;
    const EncryptionSchemaTreeNode& schema;
    std::stack<Subtree>& subtreeStack;
};

class IntentionPostVisitor final : public ExpressionVisitor {
public:
    IntentionPostVisitor(const ExpressionContext& expCtx,
                         const EncryptionSchemaTreeNode& schema,
                         std::stack<Subtree>& subtreeStack)
        : expCtx(expCtx), schema(schema), subtreeStack(subtreeStack) {}

    Intention didSetIntention = Intention::NotMarked;

private:
    void visit(ExpressionConstant*) final {}
    void visit(ExpressionAbs*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionAdd*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionAllElementsTrue*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionAnd*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionAnyElementTrue*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionArray*) final {
        // As documented in the PreVisitor we only sometimes push an Evaluated output type Subtree
        // onto the stack. If we did, we should find it on top and exit our Subtree. If we did
        // not, we should find a Compared output type Subtree on top since the Compared struct is
        // the mechanism for communicating when this special behavior should be triggered.
        if (stdx::get_if<Subtree::Evaluated>(&subtreeStack.top().output)) {
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
        } else {
            invariant(stdx::get_if<Subtree::Compared>(&subtreeStack.top().output));
        }
    }
    void visit(ExpressionArrayElemAt*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionObjectToArray*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionArrayToObject*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionCeil*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionCoerceToBool*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionCompare* compare) final {
        switch (compare->getOp()) {
            case ExpressionCompare::EQ:
            case ExpressionCompare::NE: {
                didSetIntention =
                    exitSubtree<Subtree::Compared>(expCtx, subtreeStack) || didSetIntention;
                return;
            }
            default: {
                didSetIntention =
                    exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
                return;
            }
        }
    }
    void visit(ExpressionConcat*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionConcatArrays*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionCond*) final {}
    void visit(ExpressionDateFromString*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDateFromParts*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDateToParts*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDateToString*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDivide*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionExp*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFieldPath*) final {}
    void visit(ExpressionFilter*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFloor*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIfNull*) final {}
    void visit(ExpressionIn* in) final {
        // See the comment in the PreVisitor about why we have to special case an array literal.
        if (dynamic_cast<ExpressionArray*>(in->getOperandList()[1].get())) {
            didSetIntention =
                exitSubtree<Subtree::Compared>(expCtx, subtreeStack) || didSetIntention;
        } else {
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
        }
    }
    void visit(ExpressionIndexOfArray*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIndexOfBytes*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIndexOfCP*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionLet*) final {}
    void visit(ExpressionLn*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionLog*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionLog10*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionMap*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionMeta*) final {}
    void visit(ExpressionMod*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionMultiply*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionNot*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionObject*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionOr*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionPow*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionRange*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionReduce*) final {}
    void visit(ExpressionSetDifference*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSetEquals*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSetIntersection*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSetIsSubset*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSetUnion*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSize*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionReverseArray*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSlice*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIsArray*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionRound*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSplit*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSqrt*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionStrcasecmp*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSubstrBytes*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSubstrCP*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionStrLenBytes*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionStrLenCP*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSubtract*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSwitch*) final {
        // We are exiting the default branch which is part of the parent Subtree so no work is
        // required here.
    }
    void visit(ExpressionToLower*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionToUpper*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionTrim*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionTrunc*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionType*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionZip*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionConvert*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionRegexFind*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionRegexFindAll*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionRegexMatch*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionCosine*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSine*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionTangent*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionArcCosine*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionArcSine*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionArcTangent*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionArcTangent2*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionHyperbolicArcTangent*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionHyperbolicArcCosine*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionHyperbolicArcSine*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionHyperbolicTangent*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionHyperbolicCosine*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionHyperbolicSine*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDegreesToRadians*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionRadiansToDegrees*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDayOfMonth*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDayOfWeek*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDayOfYear*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionHour*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionMillisecond*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionMinute*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionMonth*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSecond*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionWeek*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIsoWeekYear*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIsoDayOfWeek*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIsoWeek*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionYear*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulator<AccumulatorAvg>*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulator<AccumulatorMax>*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulator<AccumulatorMin>*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulator<AccumulatorSum>*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionTests::Testable*) final {}

    const ExpressionContext& expCtx;
    const EncryptionSchemaTreeNode& schema;
    std::stack<Subtree>& subtreeStack;
};

class IntentionWalker final {
public:
    IntentionWalker(const ExpressionContext& expCtx,
                    const EncryptionSchemaTreeNode& schema,
                    bool expressionOutputIsCompared)
        : expCtx(expCtx), schema(schema) {
        // Before walking, enter the outermost Subtree.
        enterSubtree(expressionOutputIsCompared ? decltype(Subtree::output)(Subtree::Compared{})
                                                : Subtree::Forwarded{},
                     subtreeStack);
    }
    Intention exitOutermostSubtree(bool expressionOutputIsCompared) {
        // When walking is complete, exit the outermost Subtree and report whether any fields were
        // marked in the execution of the walker.
        auto rootSubtreeSetIntention = expressionOutputIsCompared
            ? exitSubtree<Subtree::Compared>(expCtx, subtreeStack)
            : exitSubtree<Subtree::Forwarded>(expCtx, subtreeStack);
        return rootSubtreeSetIntention || intentionPostVisitor.didSetIntention ||
            intentionInVisitor.didSetIntention;
    }

    void preVisit(Expression* expression) {
        expression->acceptVisitor(&intentionPreVisitor);
    }
    void inVisit(unsigned long long count, Expression* expression) {
        intentionInVisitor.numChildrenVisited = count;
        expression->acceptVisitor(&intentionInVisitor);
    }
    void postVisit(Expression* expression) {
        expression->acceptVisitor(&intentionPostVisitor);
    }


private:
    const ExpressionContext& expCtx;
    const EncryptionSchemaTreeNode& schema;
    std::stack<Subtree> subtreeStack;

    IntentionPreVisitor intentionPreVisitor{expCtx, schema, subtreeStack};
    IntentionInVisitor intentionInVisitor{expCtx, schema, subtreeStack};
    IntentionPostVisitor intentionPostVisitor{expCtx, schema, subtreeStack};
};

}  // namespace

Intention mark(const ExpressionContext& expCtx,
               const EncryptionSchemaTreeNode& schema,
               Expression* expression,
               bool expressionOutputIsCompared) {
    IntentionWalker walker{expCtx, schema, expressionOutputIsCompared};
    expression_walker::walk(&walker, expression);
    return walker.exitOutermostSubtree(expressionOutputIsCompared);
}

}  // namespace mongo::aggregate_expression_intender
