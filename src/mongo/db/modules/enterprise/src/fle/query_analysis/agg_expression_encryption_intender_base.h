/**
 * Copyright (C) 2022-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>

#include "encryption_schema_tree.h"
#include "mongo/base/string_data.h"
#include "mongo/db/matcher/expression_leaf.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/expression_from_accumulator_quantile.h"
#include "mongo/db/pipeline/expression_visitor.h"
#include "mongo/db/pipeline/expression_walker.h"
#include "mongo/s/commands/sharding_expressions.h"
#include "query_analysis.h"

namespace mongo {
/**
 * Indicates whether or not references to FLE 2-encrypted fields are allowed within an expression.
 * The value of this enum should be chosen based on the server-side support for completing rewrites
 * of the expression. It should be disallowed here if the server-side does not support the rewrite.
 * This is not used by the base class, but may be used by derived classes.
 */
enum class FLE2FieldRefExpr { allowed, disallowed };
namespace aggregate_expression_intender {

/**
 * Indicates whether or not mark() actually inserted any intent-to-encrypt markers, since they are
 * not always necessary.
 */
enum class [[nodiscard]] Intention : bool{Marked = true, NotMarked = false};

inline Intention operator||(Intention a, Intention b) {
    if (a == Intention::Marked || b == Intention::Marked) {
        return Intention::Marked;
    } else {
        return Intention::NotMarked;
    }
}
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

        // Whether or not an encrypted field path is permitted in the expression tree is context-
        // specific for FLE 2. As above, we use this member variable to track whether or not we are
        // in a special circumstance where we can allow a field path, and we use a pointer to the
        // specific ExpressionFieldPath we know is allowed to add an extra layer of defense.
        ExpressionFieldPath* temporarilyPermittedEncryptedFieldPath{nullptr};

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

        std::variant<Unknown, NotEncrypted, Encrypted> state;
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

    std::variant<Forwarded, Compared, Evaluated> output;
};

std::string toString(const decltype(Subtree::output)& outputType);

template <typename T>
std::string toString() {
    if constexpr (std::is_same_v<T, Subtree::Forwarded>) {
        return "Subtree::Forwarded";
    } else if constexpr (std::is_same_v<T, Subtree::Evaluated>) {
        return "Subtree::Evaluated";
    } else if constexpr (std::is_same_v<T, Subtree::Compared>) {
        return "Subtree::Compared";
    }
}

void rewriteLiteralToIntent(const ExpressionContext& expCtx,
                            const ResolvedEncryptionInfo& encryptedType,
                            ExpressionConstant* literal);

void enterSubtree(decltype(Subtree::output) outputType, std::stack<Subtree>& subtreeStack);

template <typename Out>
void exitSubtreeNoReplacement(const ExpressionContext& expCtx, std::stack<Subtree>& subtreeStack) {
    // It's really easy to push and forget to pop (enter but not exit). As a layer of safety we
    // verify that you are popping off the stack the type you expect to be popping.
    using namespace fmt::literals;
    visit(
        [](auto&& output) {
            using OutputType = std::decay_t<decltype(output)>;
            if constexpr (!std::is_same_v<OutputType, Out>) {
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
}

template <typename Out>
Intention exitSubtree(const ExpressionContext& expCtx, std::stack<Subtree>& subtreeStack) {
    bool literalRewritten = false;
    if (auto compared = get_if<Subtree::Compared>(&subtreeStack.top().output))
        if (auto encrypted = get_if<Subtree::Compared::Encrypted>(&compared->state)) {
            for (auto&& literal : compared->literals)
                rewriteLiteralToIntent(expCtx, encrypted->type, literal);
            literalRewritten = compared->literals.size() > 0;
        }
    exitSubtreeNoReplacement<Out>(expCtx, subtreeStack);
    return literalRewritten ? Intention::Marked : Intention::NotMarked;
}

[[noreturn]] void uassertedEncryptedInEvaluatedContext(FieldPath& currentField,
                                                       StringData evaluatedBy);

[[noreturn]] void uassertedEncryptedUnencryptedMismatch(
    const FieldPath& currentField,
    std::vector<FieldPath>& comparedFields,
    std::vector<StringData> comparedEvaluations);

[[noreturn]] void uassertedUnencryptedEncryptedMismatch(
    const FieldPath& currentField, const std::vector<FieldPath>& comparedFields);

[[noreturn]] void uassertedEncryptedEncryptedMismatch(const FieldPath& currentField,
                                                      const std::vector<FieldPath>& comparedFields);

[[noreturn]] void uassertedComparisonOfRandomlyEncrypted(const FieldPath& currentField);
[[noreturn]] void uassertedComparisonFLE2EncryptedFields(const FieldPath& fieldPath0,
                                                         const FieldPath& fieldPath1);

void ensureFLE2EncryptedFieldComparedToConstant(ExpressionFieldPath* encryptedFieldPath,
                                                Expression* comparedTo);

[[noreturn]] void uassertedEvaluationInComparedEncryptedSubtree(
    StringData evaluation, const std::vector<FieldPath>& comparedFields);

[[noreturn]] void uassertedForbiddenVariable(StringData variableName);

auto getEncryptionTypeForPathEnsureNotPrefix(const EncryptionSchemaTreeNode& schema,
                                             const ExpressionFieldPath& fieldPath);

decltype(Subtree::Compared::state) reconcileAgainstUnknownEncryption(
    const EncryptionSchemaTreeNode& schema, const ExpressionFieldPath& fieldPath);


decltype(Subtree::Compared::state) attemptReconcilingAgainstNoEncryption(
    const EncryptionSchemaTreeNode& schema,
    const ExpressionFieldPath& fieldPath,
    const std::vector<FieldPath>& comparedFields,
    std::vector<StringData> comparedEvaluations);
decltype(Subtree::Compared::state) attemptReconcilingAgainstEncryption(
    const EncryptionSchemaTreeNode& schema,
    const ExpressionFieldPath& fieldPath,
    const std::vector<FieldPath>& comparedFields,
    const ResolvedEncryptionInfo& currentEncryptedType);

void errorIfEncryptedFieldFoundInEvaluated(const EncryptionSchemaTreeNode& schema,
                                           const ExpressionFieldPath& fieldPath,
                                           Subtree::Evaluated* evaluated);

void attemptReconcilingFieldEncryptionInCompared(const EncryptionSchemaTreeNode& schema,
                                                 const ExpressionFieldPath& fieldPath,
                                                 Subtree::Compared* compared);

void attemptReconcilingFieldEncryption(const EncryptionSchemaTreeNode& schema,
                                       const ExpressionFieldPath& fieldPath,
                                       std::stack<Subtree>& subtreeStack);

/**
 * We must accomplish a few maintenance tasks here if we are in a Compared output Subtree:
 * * We must assert that we are in an Unknown or NotEncrypted state.
 * * We must transition to a NotEncrypted state.
 * * We must add the reasoning behind this call to the evaluated vector which provides explanations
 *   for error messages.
 */
void ensureNotEncrypted(StringData reason, std::stack<Subtree>& subtreeStack);

/**
 * This calls ensureNotEncrypted while providing the upcoming Evaluated Subtree's by string as the
 * reasoning behind the assertion that this Subtree is NotEncrypted or not Compared. Regardless of
 * our current Subtree output type, we then enter a new Evaluated Subtree.
 */
void ensureNotEncryptedEnterEval(StringData evaluation, std::stack<Subtree>& subtreeStack);

/**
 * Here we attempt to reconcile against a variable access. All user-bound variables are unencrypted
 * since their definitions were walked in an Evaluated output type. All existing system variables
 * are also deemed unencrypted with the exception of CURRENT and ROOT which refer to the whole
 * document (CURRENT is not rebindable in FLE).
 */
void reconcileVariableAccess(const ExpressionFieldPath& variableFieldPath,
                             std::stack<Subtree>& subtreeStack);

/**
 * Helper to determine if a comparison expression has exactly a field path and a constant. Returns
 * a pointer to each of the children of that type, or nullptrs if the types are not correct.
 */
std::pair<ExpressionFieldPath*, ExpressionConstant*> getFieldPathAndConstantFromExpression(
    ExpressionNary* expr);

/**
 * If this expression is holding an encrypted placeholder, assume it is correct and was put there by
 * a previous rewrite pass. Return the encrypted path or if it is not, return nullptr. Only
 * ExpressionCompare can hold placeholders.
 */
ExpressionFieldPath* getFieldpathForEncryptedCompare(ExpressionCompare* compare);

/**
 * Helper to determine if a value is an encrypted payload.
 */
bool isEncryptedPayload(Value val);

/**
 * Expression visitor base class for encryption. Implements generic traversal where necessary,
 * but should not be instantiated -- assumes all values and paths are unencrypted.
 *
 * Implements and expects visitors to implement 'visit' for all expressions, including internal
 * ones and those that are not likely to come up in encrypted contexts. This is to avoid duplicating
 * any validation logic here and during normal expression parsing.
 */
class IntentionPreVisitorBase : public ExpressionMutableVisitor {
public:
    IntentionPreVisitorBase(ExpressionContext* expCtx,
                            const EncryptionSchemaTreeNode& schema,
                            std::stack<Subtree>& subtreeStack,
                            FLE2FieldRefExpr fieldRefSupported)
        : expCtx(expCtx),
          schema(schema),
          subtreeStack(subtreeStack),
          fieldRefSupported(fieldRefSupported) {}

protected:
    void visit(ExpressionConstant* constant) override {
        if (auto compared = get_if<Subtree::Compared>(&subtreeStack.top().output))
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
    void visit(ExpressionAnd*) override {
        ensureNotEncryptedEnterEval("a conjunction", subtreeStack);
    }
    void visit(ExpressionAnyElementTrue*) final {
        ensureNotEncryptedEnterEval("an 'any elements true' expression", subtreeStack);
    }
    void visit(ExpressionArray* array) final;
    void visit(ExpressionArrayElemAt*) final {
        ensureNotEncryptedEnterEval("array indexing", subtreeStack);
    }
    void visit(ExpressionBitAnd*) final {
        ensureNotEncryptedEnterEval("a bitwise AND operation", subtreeStack);
    }
    void visit(ExpressionBitOr*) final {
        ensureNotEncryptedEnterEval("a bitwise AND operation", subtreeStack);
    }
    void visit(ExpressionBitXor*) final {
        ensureNotEncryptedEnterEval("a bitwise AND operation", subtreeStack);
    }
    void visit(ExpressionBitNot*) final {
        ensureNotEncryptedEnterEval("array indexing", subtreeStack);
    }
    void visit(ExpressionFirst*) final {
        ensureNotEncryptedEnterEval("array indexing (first element)", subtreeStack);
    }
    void visit(ExpressionLast*) final {
        ensureNotEncryptedEnterEval("array indexing (last element)", subtreeStack);
    }
    void visit(ExpressionObjectToArray*) final {
        ensureNotEncryptedEnterEval("an object to array conversion", subtreeStack);
    }
    void visit(ExpressionArrayToObject*) final {
        ensureNotEncryptedEnterEval("an array to object conversion", subtreeStack);
    }
    void visit(ExpressionBsonSize*) final {
        ensureNotEncryptedEnterEval("an object bsonSize calculation", subtreeStack);
    }
    void visit(ExpressionCeil*) final {
        ensureNotEncryptedEnterEval("a ceiling calculation", subtreeStack);
    }
    void visit(ExpressionCoerceToBool*) final {
        ensureNotEncryptedEnterEval("a coercion to boolean", subtreeStack);
    }
    void visit(ExpressionCompare* compare) override {
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

                enterSubtree(comparedSubtree, subtreeStack);
                return;
            }
            case ExpressionCompare::GT:
                if (auto fp = getFieldpathForEncryptedCompare(compare)) {
                    Subtree::Compared comparedSubtree;
                    comparedSubtree.temporarilyPermittedEncryptedFieldPath = fp;
                    enterSubtree(comparedSubtree, subtreeStack);
                } else {
                    ensureNotEncryptedEnterEval("a greater than comparison", subtreeStack);
                }
                return;
            case ExpressionCompare::GTE:
                if (auto fp = getFieldpathForEncryptedCompare(compare)) {
                    Subtree::Compared comparedSubtree;
                    comparedSubtree.temporarilyPermittedEncryptedFieldPath = fp;
                    enterSubtree(comparedSubtree, subtreeStack);
                } else {
                    ensureNotEncryptedEnterEval("a greater than or equal comparison", subtreeStack);
                }
                return;
            case ExpressionCompare::LT:
                if (auto fp = getFieldpathForEncryptedCompare(compare)) {
                    Subtree::Compared comparedSubtree;
                    comparedSubtree.temporarilyPermittedEncryptedFieldPath = fp;
                    enterSubtree(comparedSubtree, subtreeStack);
                } else {
                    ensureNotEncryptedEnterEval("a less than comparison", subtreeStack);
                }
                return;
            case ExpressionCompare::LTE:
                if (auto fp = getFieldpathForEncryptedCompare(compare)) {
                    Subtree::Compared comparedSubtree;
                    comparedSubtree.temporarilyPermittedEncryptedFieldPath = fp;
                    enterSubtree(comparedSubtree, subtreeStack);
                } else {
                    ensureNotEncryptedEnterEval("a less than or equal comparison", subtreeStack);
                }
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
    void visit(ExpressionDateAdd*) final {
        ensureNotEncryptedEnterEval("date add function", subtreeStack);
    }
    void visit(ExpressionDateDiff*) final {
        ensureNotEncryptedEnterEval("date diff function", subtreeStack);
    }
    void visit(ExpressionDateFromString*) final {
        ensureNotEncryptedEnterEval("date from string function", subtreeStack);
    }
    void visit(ExpressionDateFromParts*) final {
        ensureNotEncryptedEnterEval("date from parts function", subtreeStack);
    }
    void visit(ExpressionDateSubtract*) final {
        ensureNotEncryptedEnterEval("date subtract function", subtreeStack);
    }
    void visit(ExpressionDateToParts*) final {
        ensureNotEncryptedEnterEval("date to parts function", subtreeStack);
    }
    void visit(ExpressionDateToString*) final {
        ensureNotEncryptedEnterEval("date to string function", subtreeStack);
    }
    void visit(ExpressionDateTrunc*) final {
        ensureNotEncryptedEnterEval("date truncation function", subtreeStack);
    }
    void visit(ExpressionDivide*) final {
        ensureNotEncryptedEnterEval("division", subtreeStack);
    }
    void visit(ExpressionExp*) final {
        ensureNotEncryptedEnterEval("an exponentiation", subtreeStack);
    }
    void visit(ExpressionFieldPath* fieldPath) final;
    void visit(ExpressionFilter*) final {
        ensureNotEncryptedEnterEval("an array filter", subtreeStack);
    }
    void visit(ExpressionFloor*) final {
        ensureNotEncryptedEnterEval("a floor calculation", subtreeStack);
    }
    void visit(ExpressionFunction*) final {
        ensureNotEncryptedEnterEval("a $function expression", subtreeStack);
    }
    void visit(ExpressionGetField*) final {
        ensureNotEncryptedEnterEval("a $getField expression", subtreeStack);
    }
    void visit(ExpressionSetField*) final {
        ensureNotEncryptedEnterEval("a $setField expression", subtreeStack);
    }
    void visit(ExpressionToHashedIndexKey*) final {
        ensureNotEncryptedEnterEval("a $hash expression", subtreeStack);
    }
    void visit(ExpressionIfNull*) final {
        // If $ifNull appears under a comparison subtree, then both arguments to $ifNull should be
        // marked or assert just as if they were the direct descendant of the grandparent
        // comparison.
    }
    void visit(ExpressionIn* in) override;
    void visit(ExpressionIndexOfArray*) final {
        ensureNotEncryptedEnterEval("an array find", subtreeStack);
    }
    void visit(ExpressionIndexOfBytes*) final {
        ensureNotEncryptedEnterEval("a byte-based string find", subtreeStack);
    }
    void visit(ExpressionIndexOfCP*) final {
        ensureNotEncryptedEnterEval("a code-point-based string find", subtreeStack);
    }
    void visit(ExpressionInternalJsEmit*) final {
        ensureNotEncryptedEnterEval("an internal JS emit expression", subtreeStack);
    }
    void visit(ExpressionInternalFindElemMatch*) override {
        ensureNotEncryptedEnterEval("an internal find $elemMatch expression", subtreeStack);
    }
    void visit(ExpressionInternalFindPositional*) override {
        ensureNotEncryptedEnterEval("an internal find positional expression", subtreeStack);
    }
    void visit(ExpressionInternalFindSlice*) override {
        ensureNotEncryptedEnterEval("an internal find $slice expression", subtreeStack);
    }
    void visit(ExpressionIsNumber*) final {
        ensureNotEncryptedEnterEval("a numeric-type checker", subtreeStack);
    }
    void visit(ExpressionLet* let) final {
        for (auto&& [unused, nameAndExpression] : let->getVariableMap())
            if (auto&& [name, unused] = nameAndExpression; name == "CURRENT")
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
    void visit(ExpressionInternalFLEEqual*) final {
        ensureNotEncryptedEnterEval("a fle equal match", subtreeStack);
    }
    void visit(ExpressionInternalFLEBetween*) final {
        ensureNotEncryptedEnterEval("a fle between match", subtreeStack);
    }
    void visit(ExpressionInternalRawSortKey*) final {
        ensureNotEncryptedEnterEval("a raw sort key metadata access", subtreeStack);
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
    void visit(ExpressionRandom*) final {
        ensureNotEncryptedEnterEval("a $rand expression", subtreeStack);
    }
    void visit(ExpressionRange*) final {
        ensureNotEncryptedEnterEval("a numeric sequence generator", subtreeStack);
    }
    void visit(ExpressionReduce*) final {
        enterSubtree(Subtree::Evaluated{"a reduce initializer"}, subtreeStack);
    }
    void visit(ExpressionReplaceOne*) final {
        ensureNotEncryptedEnterEval("a string replaceOne operation", subtreeStack);
    }
    void visit(ExpressionReplaceAll*) final {
        ensureNotEncryptedEnterEval("a string replaceAll operation", subtreeStack);
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
    void visit(ExpressionSortArray*) final {
        ensureNotEncryptedEnterEval("an array sorting", subtreeStack);
    }
    void visit(ExpressionSlice*) final {
        ensureNotEncryptedEnterEval("an array subset operation", subtreeStack);
    }
    void visit(ExpressionIsArray*) final {
        ensureNotEncryptedEnterEval("an array type determination", subtreeStack);
    }
    void visit(ExpressionInternalFindAllValuesAtPath*) final {
        ensureNotEncryptedEnterEval("an array deep unwinding operation", subtreeStack);
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
    void visit(ExpressionBinarySize*) final {
        ensureNotEncryptedEnterEval("a byte-based string or BinData length determination",
                                    subtreeStack);
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
    void visit(ExpressionTestApiVersion*) final {
        enterSubtree(Subtree::Evaluated{"an API version evaluation"}, subtreeStack);
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
    void visit(ExpressionFromAccumulatorN<AccumulatorFirstN>*) final {
        ensureNotEncryptedEnterEval("an aggregation of the first 'n' values", subtreeStack);
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorLastN>*) final {
        ensureNotEncryptedEnterEval("an aggregation of the last 'n' values", subtreeStack);
    }
    void visit(ExpressionFromAccumulator<AccumulatorMax>*) final {
        ensureNotEncryptedEnterEval("a maximum aggregation", subtreeStack);
    }
    void visit(ExpressionFromAccumulator<AccumulatorMin>*) final {
        ensureNotEncryptedEnterEval("a minimum aggregation", subtreeStack);
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorMaxN>*) final {
        ensureNotEncryptedEnterEval("a maximum aggregation of up to 'n' values", subtreeStack);
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorMinN>*) final {
        ensureNotEncryptedEnterEval("a minimum aggregation of up to 'n' values", subtreeStack);
    }
    void visit(ExpressionFromAccumulatorQuantile<AccumulatorMedian>*) final {
        ensureNotEncryptedEnterEval("a percentile calculation", subtreeStack);
    }
    void visit(ExpressionFromAccumulatorQuantile<AccumulatorPercentile>*) final {
        ensureNotEncryptedEnterEval("a percentile calculation", subtreeStack);
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
    void visit(ExpressionTsSecond*) final {
        ensureNotEncryptedEnterEval("a timestamp second component extractor", subtreeStack);
    }
    void visit(ExpressionTsIncrement*) final {
        ensureNotEncryptedEnterEval("a timestamp increment component extractor", subtreeStack);
    }

    void visit(ExpressionTests::Testable*) final {}
    void visit(ExpressionInternalOwningShard*) final {
        ensureNotEncryptedEnterEval("a shard id computing operation", subtreeStack);
    }
    void visit(ExpressionInternalIndexKey*) final {
        ensureNotEncryptedEnterEval("an index keys objects generation computing operation",
                                    subtreeStack);
    }
    void visit(ExpressionInternalKeyStringValue*) final {
        ensureNotEncryptedEnterEval("a key string value generation computing operation",
                                    subtreeStack);
    }

    bool isEncryptedFieldPath(ExpressionFieldPath* fieldPathExpr) {
        if (fieldPathExpr) {
            auto ref = fieldPathExpr->getFieldPathWithoutCurrentPrefix().fullPath();
            return schema.getEncryptionMetadataForPath(FieldRef{ref}) ||
                schema.mayContainEncryptedNodeBelowPrefix(FieldRef{ref});
        }
        return false;
    };
    ExpressionContext* expCtx;
    const EncryptionSchemaTreeNode& schema;
    std::stack<Subtree>& subtreeStack;
    FLE2FieldRefExpr fieldRefSupported;
};

class IntentionInVisitorBase : public ExpressionMutableVisitor {
public:
    IntentionInVisitorBase(const ExpressionContext& expCtx,
                           const EncryptionSchemaTreeNode& schema,
                           std::stack<Subtree>& subtreeStack)
        : expCtx(expCtx), schema(schema), subtreeStack(subtreeStack) {}

protected:
    void visit(ExpressionConstant*) override {}
    void visit(ExpressionAbs*) override {}
    void visit(ExpressionAdd*) override {}
    void visit(ExpressionAllElementsTrue*) override {}
    void visit(ExpressionAnd*) override {}
    void visit(ExpressionAnyElementTrue*) override {}
    void visit(ExpressionArray*) override {}
    void visit(ExpressionArrayElemAt*) override {}
    void visit(ExpressionBitAnd*) override {}
    void visit(ExpressionBitOr*) override {}
    void visit(ExpressionBitXor*) override {}
    void visit(ExpressionBitNot*) override {}
    void visit(ExpressionFirst*) override {}
    void visit(ExpressionLast*) override {}
    void visit(ExpressionObjectToArray*) override {}
    void visit(ExpressionArrayToObject*) override {}
    void visit(ExpressionBsonSize*) override {}
    void visit(ExpressionCeil*) override {}
    void visit(ExpressionCoerceToBool*) override {}
    void visit(ExpressionCompare*) override {}
    void visit(ExpressionConcat*) override {}
    void visit(ExpressionConcatArrays*) override {}
    void visit(ExpressionCond*) override {
        if (numChildrenVisited == 1ull)
            // We need to exit the Evaluated output Subtree for if child.
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
        // The then and else children should be part of the parent Subtree.
    }
    void visit(ExpressionDateAdd*) override {}
    void visit(ExpressionDateDiff*) override {}
    void visit(ExpressionDateFromString*) override {}
    void visit(ExpressionDateFromParts*) override {}
    void visit(ExpressionDateSubtract*) override {}
    void visit(ExpressionDateToParts*) override {}
    void visit(ExpressionDateToString*) override {}
    void visit(ExpressionDateTrunc*) override {}
    void visit(ExpressionDivide*) override {}
    void visit(ExpressionExp*) override {}
    void visit(ExpressionFieldPath*) override {}
    void visit(ExpressionFilter*) override {}
    void visit(ExpressionFloor*) override {}
    void visit(ExpressionFunction*) override {}
    void visit(ExpressionGetField*) override {}
    void visit(ExpressionSetField*) override {}
    void visit(ExpressionTestApiVersion*) override {}
    void visit(ExpressionToHashedIndexKey*) override {}
    void visit(ExpressionIfNull*) override {}
    void visit(ExpressionIn* in) override {
        if (auto arrayLiteral = dynamic_cast<ExpressionArray*>(in->getOperandList()[1].get())) {
            // There must be a subtree with Compared output type at the top since we just put it
            // there.
            auto comparedSubtree = get_if<Subtree::Compared>(&subtreeStack.top().output);
            invariant(comparedSubtree,
                      "$in expected to find the Subtree::Compared that it pushed onto the stack. "
                      "Perhaps a subtree forgot to pop off the stack before exiting postVisit()?");
            comparedSubtree->temporarilyPermittedArrayLiteral = arrayLiteral;
        }
    }
    void visit(ExpressionIndexOfArray*) override {}
    void visit(ExpressionIndexOfBytes*) override {}
    void visit(ExpressionIndexOfCP*) override {}
    void visit(ExpressionInternalJsEmit*) override {}
    void visit(ExpressionInternalFindElemMatch*) override {}
    void visit(ExpressionInternalFindPositional*) override {}
    void visit(ExpressionInternalFindSlice*) override {}
    void visit(ExpressionIsNumber*) override {}
    void visit(ExpressionLet* let) final {
        // The final child of a let Expression is part of the parent Subtree.
        if (numChildrenVisited == let->getChildren().size() - 1)
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionLn*) override {}
    void visit(ExpressionLog*) override {}
    void visit(ExpressionLog10*) override {}
    void visit(ExpressionInternalFLEEqual*) override {}
    void visit(ExpressionInternalFLEBetween*) override {}
    void visit(ExpressionInternalRawSortKey*) override {}
    void visit(ExpressionMap*) override {}
    void visit(ExpressionMeta*) override {}
    void visit(ExpressionMod*) override {}
    void visit(ExpressionMultiply*) override {}
    void visit(ExpressionNot*) override {}
    void visit(ExpressionObject*) override {}
    void visit(ExpressionOr*) override {}
    void visit(ExpressionPow*) override {}
    void visit(ExpressionRandom*) override {}
    void visit(ExpressionRange*) override {}
    void visit(ExpressionReduce* reduce) override {
        // As with ExpressionLet the child here is part of the parent Subtree.
        if (numChildrenVisited == reduce->getChildren().size() - 1)
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionReplaceOne*) override {}
    void visit(ExpressionReplaceAll*) override {}
    void visit(ExpressionSetDifference*) override {}
    void visit(ExpressionSetEquals*) override {}
    void visit(ExpressionSetIntersection*) override {}
    void visit(ExpressionSetIsSubset*) override {}
    void visit(ExpressionSetUnion*) override {}
    void visit(ExpressionSize*) override {}
    void visit(ExpressionReverseArray*) override {}
    void visit(ExpressionSortArray*) override {}
    void visit(ExpressionSlice*) override {}
    void visit(ExpressionIsArray*) override {}
    void visit(ExpressionInternalFindAllValuesAtPath*) override {}
    void visit(ExpressionRound*) override {}
    void visit(ExpressionSplit*) override {}
    void visit(ExpressionSqrt*) override {}
    void visit(ExpressionStrcasecmp*) override {}
    void visit(ExpressionSubstrBytes*) override {}
    void visit(ExpressionSubstrCP*) override {}
    void visit(ExpressionStrLenBytes*) override {}
    void visit(ExpressionBinarySize*) override {}
    void visit(ExpressionStrLenCP*) override {}
    void visit(ExpressionSubtract*) override {}
    void visit(ExpressionSwitch* switchExpr) override {
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
    void visit(ExpressionToLower*) override {}
    void visit(ExpressionToUpper*) override {}
    void visit(ExpressionTrim*) override {}
    void visit(ExpressionTrunc*) override {}
    void visit(ExpressionType*) override {}
    void visit(ExpressionZip*) override {}
    void visit(ExpressionConvert*) override {}
    void visit(ExpressionRegexFind*) override {}
    void visit(ExpressionRegexFindAll*) override {}
    void visit(ExpressionRegexMatch*) override {}
    void visit(ExpressionCosine*) override {}
    void visit(ExpressionSine*) override {}
    void visit(ExpressionTangent*) override {}
    void visit(ExpressionArcCosine*) override {}
    void visit(ExpressionArcSine*) override {}
    void visit(ExpressionArcTangent*) override {}
    void visit(ExpressionArcTangent2*) override {}
    void visit(ExpressionHyperbolicArcTangent*) override {}
    void visit(ExpressionHyperbolicArcCosine*) override {}
    void visit(ExpressionHyperbolicArcSine*) override {}
    void visit(ExpressionHyperbolicTangent*) override {}
    void visit(ExpressionHyperbolicCosine*) override {}
    void visit(ExpressionHyperbolicSine*) override {}
    void visit(ExpressionDegreesToRadians*) override {}
    void visit(ExpressionRadiansToDegrees*) override {}
    void visit(ExpressionDayOfMonth*) override {}
    void visit(ExpressionDayOfWeek*) override {}
    void visit(ExpressionDayOfYear*) override {}
    void visit(ExpressionHour*) override {}
    void visit(ExpressionMillisecond*) override {}
    void visit(ExpressionMinute*) override {}
    void visit(ExpressionMonth*) override {}
    void visit(ExpressionSecond*) override {}
    void visit(ExpressionWeek*) override {}
    void visit(ExpressionIsoWeekYear*) override {}
    void visit(ExpressionIsoDayOfWeek*) override {}
    void visit(ExpressionIsoWeek*) override {}
    void visit(ExpressionYear*) override {}
    void visit(ExpressionFromAccumulator<AccumulatorAvg>*) override {}
    void visit(ExpressionFromAccumulatorN<AccumulatorFirstN>*) override {}
    void visit(ExpressionFromAccumulatorN<AccumulatorLastN>*) override {}
    void visit(ExpressionFromAccumulator<AccumulatorMax>*) override {}
    void visit(ExpressionFromAccumulator<AccumulatorMin>*) override {}
    void visit(ExpressionFromAccumulatorN<AccumulatorMaxN>*) override {}
    void visit(ExpressionFromAccumulatorN<AccumulatorMinN>*) override {}
    void visit(ExpressionFromAccumulatorQuantile<AccumulatorMedian>*) override {}
    void visit(ExpressionFromAccumulatorQuantile<AccumulatorPercentile>*) override {}
    void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>*) override {}
    void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>*) override {}
    void visit(ExpressionFromAccumulator<AccumulatorSum>*) override {}
    void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>*) override {}
    void visit(ExpressionTsSecond*) override {}
    void visit(ExpressionTsIncrement*) override {}
    void visit(ExpressionTests::Testable*) override {}
    void visit(ExpressionInternalOwningShard*) override {}
    void visit(ExpressionInternalIndexKey*) override {}
    void visit(ExpressionInternalKeyStringValue*) override {}


public:
    /**
     * The number of child nodes which have already been visited for a given parent node. This is
     * set by the Walker before inVisit is called.
     */
    unsigned long long numChildrenVisited = 0ull;

    Intention didSetIntention = Intention::NotMarked;

protected:
    const ExpressionContext& expCtx;
    const EncryptionSchemaTreeNode& schema;
    std::stack<Subtree>& subtreeStack;
};

class IntentionPostVisitorBase : public ExpressionMutableVisitor {
public:
    IntentionPostVisitorBase(const ExpressionContext& expCtx,
                             const EncryptionSchemaTreeNode& schema,
                             std::stack<Subtree>& subtreeStack)
        : expCtx(expCtx), schema(schema), subtreeStack(subtreeStack) {}

    Intention didSetIntention = Intention::NotMarked;

protected:
    void visit(ExpressionConstant*) final {}
    void visit(ExpressionAbs*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionAdd*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionAllElementsTrue*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionAnd*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionAnyElementTrue*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionArray*) override {
        // As documented in the PreVisitor we only sometimes push an Evaluated output type Subtree
        // onto the stack. If we did, we should find it on top and exit our Subtree. If we did
        // not, we should find a Compared output type Subtree on top since the Compared struct is
        // the mechanism for communicating when this special behavior should be triggered.
        if (get_if<Subtree::Evaluated>(&subtreeStack.top().output)) {
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
        } else {
            invariant(get_if<Subtree::Compared>(&subtreeStack.top().output));
        }
    }
    void visit(ExpressionArrayElemAt*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionBitAnd*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionBitOr*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionBitXor*) final {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFirst*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionLast*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionObjectToArray*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionArrayToObject*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionBitNot*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionBsonSize*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionCeil*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionCoerceToBool*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionCompare* compare) override {
        switch (compare->getOp()) {
            case ExpressionCompare::EQ:
            case ExpressionCompare::NE: {
                didSetIntention =
                    exitSubtree<Subtree::Compared>(expCtx, subtreeStack) || didSetIntention;
                return;
            }
            default: {
                if (getFieldpathForEncryptedCompare(compare)) {
                    didSetIntention =
                        exitSubtree<Subtree::Compared>(expCtx, subtreeStack) || didSetIntention;
                } else {
                    didSetIntention =
                        exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
                }
                return;
            }
        }
    }
    void visit(ExpressionConcat*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionConcatArrays*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionCond*) override {}
    void visit(ExpressionDateAdd*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDateDiff*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDateFromParts*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDateFromString*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDateSubtract*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDateToParts*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDateToString*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDateTrunc*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDivide*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionExp*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFieldPath*) override {}
    void visit(ExpressionFilter*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFloor*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFunction*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionGetField*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSetField*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionTestApiVersion*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionToHashedIndexKey*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIfNull*) override {}
    void visit(ExpressionIn* in) override {
        // See the comment in the PreVisitor about why we have to special case an array literal.
        if (dynamic_cast<ExpressionArray*>(in->getOperandList()[1].get())) {
            didSetIntention =
                exitSubtree<Subtree::Compared>(expCtx, subtreeStack) || didSetIntention;
        } else {
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
        }
    }
    void visit(ExpressionIndexOfArray*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIndexOfBytes*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIsNumber*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIndexOfCP*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionInternalJsEmit*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionInternalFindElemMatch*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionInternalFindPositional*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionInternalFindSlice*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionLet*) override {}
    void visit(ExpressionLn*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionLog*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionLog10*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionInternalFLEEqual*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionInternalFLEBetween*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionInternalRawSortKey*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionMap*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionMeta*) override {}
    void visit(ExpressionMod*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionMultiply*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionNot*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionObject*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionOr*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionPow*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionRandom*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionRange*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionReduce*) override {}
    void visit(ExpressionReplaceOne*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionReplaceAll*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSetDifference*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSetEquals*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSetIntersection*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSetIsSubset*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSetUnion*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSize*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionReverseArray*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSortArray*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSlice*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIsArray*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionInternalFindAllValuesAtPath*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionRound*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSplit*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSqrt*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionStrcasecmp*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSubstrBytes*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSubstrCP*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionStrLenBytes*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionBinarySize*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionStrLenCP*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSubtract*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSwitch*) override {
        // We are exiting the default branch which is part of the parent Subtree so no work is
        // required here.
    }
    void visit(ExpressionToLower*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionToUpper*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionTrim*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionTrunc*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionType*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionZip*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionConvert*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionRegexFind*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionRegexFindAll*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionRegexMatch*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionCosine*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSine*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionTangent*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionArcCosine*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionArcSine*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionArcTangent*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionArcTangent2*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionHyperbolicArcTangent*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionHyperbolicArcCosine*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionHyperbolicArcSine*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionHyperbolicTangent*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionHyperbolicCosine*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionHyperbolicSine*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDegreesToRadians*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionRadiansToDegrees*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDayOfMonth*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDayOfWeek*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionDayOfYear*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionHour*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionMillisecond*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionMinute*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionMonth*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionSecond*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionWeek*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIsoWeekYear*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIsoDayOfWeek*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionIsoWeek*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionYear*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulator<AccumulatorAvg>*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorFirstN>*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorLastN>*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulator<AccumulatorMax>*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulator<AccumulatorMin>*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorMaxN>*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulatorN<AccumulatorMinN>*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulatorQuantile<AccumulatorMedian>*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulatorQuantile<AccumulatorPercentile>*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulator<AccumulatorSum>*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionTsSecond*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionTsIncrement*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionInternalOwningShard*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionInternalIndexKey*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    void visit(ExpressionInternalKeyStringValue*) override {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }


    void visit(ExpressionTests::Testable*) override {}

    const ExpressionContext& expCtx;
    const EncryptionSchemaTreeNode& schema;
    std::stack<Subtree>& subtreeStack;
};

class AggExprEncryptionIntentionWalkerBase {
public:
    AggExprEncryptionIntentionWalkerBase(ExpressionContext* expCtx,
                                         const EncryptionSchemaTreeNode& schema,
                                         bool expressionOutputIsCompared)
        : expCtx(expCtx), schema(schema) {
        // Before walking, enter the outermost Subtree.
        enterSubtree(expressionOutputIsCompared ? decltype(Subtree::output)(Subtree::Compared{})
                                                : Subtree::Forwarded{},
                     subtreeStack);
    }
    virtual Intention exitOutermostSubtree(bool expressionOutputIsCompared) {
        // When walking is complete, exit the outermost Subtree and report whether any fields were
        // marked in the execution of the walker.
        auto rootSubtreeSetIntention = expressionOutputIsCompared
            ? exitSubtree<Subtree::Compared>(*expCtx, subtreeStack)
            : exitSubtree<Subtree::Forwarded>(*expCtx, subtreeStack);
        return rootSubtreeSetIntention || getPostVisitor()->didSetIntention ||
            getInVisitor()->didSetIntention;
    }

    void preVisit(Expression* expression) {
        expression->acceptVisitor(getPreVisitor());
    }
    void inVisit(unsigned long long count, Expression* expression) {
        getInVisitor()->numChildrenVisited = count;
        expression->acceptVisitor(getInVisitor());
    }
    void postVisit(Expression* expression) {
        expression->acceptVisitor(getPostVisitor());
    }

protected:
    ExpressionContext* expCtx;
    const EncryptionSchemaTreeNode& schema;
    std::stack<Subtree> subtreeStack;

private:
    virtual IntentionPreVisitorBase* getPreVisitor() = 0;
    virtual IntentionInVisitorBase* getInVisitor() = 0;
    virtual IntentionPostVisitorBase* getPostVisitor() = 0;
};

}  // namespace aggregate_expression_intender
}  // namespace mongo
