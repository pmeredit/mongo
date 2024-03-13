/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <boost/optional.hpp>

#include "encryption_schema_tree.h"
#include "mongo/base/string_data.h"
#include "mongo/db/matcher/expression_leaf.h"
#include "mongo/db/pipeline/expression.h"
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
    virtual void visit(ExpressionConstant* constant) {
        if (auto compared = get_if<Subtree::Compared>(&subtreeStack.top().output))
            compared->literals.push_back(constant);
    }
    virtual void visit(ExpressionAbs*) final {
        ensureNotEncryptedEnterEval("an absolute value calculation", subtreeStack);
    }
    virtual void visit(ExpressionAdd*) final {
        ensureNotEncryptedEnterEval("an addition calculation", subtreeStack);
    }
    virtual void visit(ExpressionAllElementsTrue*) final {
        ensureNotEncryptedEnterEval("an 'all elements true' expression", subtreeStack);
    }
    virtual void visit(ExpressionAnd*) {
        ensureNotEncryptedEnterEval("a conjunction", subtreeStack);
    }
    virtual void visit(ExpressionAnyElementTrue*) final {
        ensureNotEncryptedEnterEval("an 'any elements true' expression", subtreeStack);
    }
    virtual void visit(ExpressionArray* array) final;
    virtual void visit(ExpressionArrayElemAt*) final {
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
    virtual void visit(ExpressionBitNot*) final {
        ensureNotEncryptedEnterEval("array indexing", subtreeStack);
    }
    virtual void visit(ExpressionFirst*) final {
        ensureNotEncryptedEnterEval("array indexing (first element)", subtreeStack);
    }
    virtual void visit(ExpressionLast*) final {
        ensureNotEncryptedEnterEval("array indexing (last element)", subtreeStack);
    }
    virtual void visit(ExpressionObjectToArray*) final {
        ensureNotEncryptedEnterEval("an object to array conversion", subtreeStack);
    }
    virtual void visit(ExpressionArrayToObject*) final {
        ensureNotEncryptedEnterEval("an array to object conversion", subtreeStack);
    }
    virtual void visit(ExpressionBsonSize*) final {
        ensureNotEncryptedEnterEval("an object bsonSize calculation", subtreeStack);
    }
    virtual void visit(ExpressionCeil*) final {
        ensureNotEncryptedEnterEval("a ceiling calculation", subtreeStack);
    }
    virtual void visit(ExpressionCoerceToBool*) final {
        ensureNotEncryptedEnterEval("a coercion to boolean", subtreeStack);
    }
    virtual void visit(ExpressionCompare* compare) {
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
    virtual void visit(ExpressionConcat*) final {
        ensureNotEncryptedEnterEval("string concatination", subtreeStack);
    }
    virtual void visit(ExpressionConcatArrays*) final {
        ensureNotEncryptedEnterEval("array concatination", subtreeStack);
    }
    virtual void visit(ExpressionCond*) final {
        // We need to enter an Evaluated Subtree for the first child of the $cond (if).
        enterSubtree(Subtree::Evaluated{"a boolean conditional"}, subtreeStack);
    }
    virtual void visit(ExpressionDateAdd*) final {
        ensureNotEncryptedEnterEval("date add function", subtreeStack);
    }
    virtual void visit(ExpressionDateDiff*) final {
        ensureNotEncryptedEnterEval("date diff function", subtreeStack);
    }
    virtual void visit(ExpressionDateFromString*) final {
        ensureNotEncryptedEnterEval("date from string function", subtreeStack);
    }
    virtual void visit(ExpressionDateFromParts*) final {
        ensureNotEncryptedEnterEval("date from parts function", subtreeStack);
    }
    virtual void visit(ExpressionDateSubtract*) final {
        ensureNotEncryptedEnterEval("date subtract function", subtreeStack);
    }
    virtual void visit(ExpressionDateToParts*) final {
        ensureNotEncryptedEnterEval("date to parts function", subtreeStack);
    }
    virtual void visit(ExpressionDateToString*) final {
        ensureNotEncryptedEnterEval("date to string function", subtreeStack);
    }
    virtual void visit(ExpressionDateTrunc*) final {
        ensureNotEncryptedEnterEval("date truncation function", subtreeStack);
    }
    virtual void visit(ExpressionDivide*) final {
        ensureNotEncryptedEnterEval("division", subtreeStack);
    }
    virtual void visit(ExpressionExp*) final {
        ensureNotEncryptedEnterEval("an exponentiation", subtreeStack);
    }
    virtual void visit(ExpressionFieldPath* fieldPath) final;
    virtual void visit(ExpressionFilter*) final {
        ensureNotEncryptedEnterEval("an array filter", subtreeStack);
    }
    virtual void visit(ExpressionFloor*) final {
        ensureNotEncryptedEnterEval("a floor calculation", subtreeStack);
    }
    virtual void visit(ExpressionFunction*) final {
        ensureNotEncryptedEnterEval("a $function expression", subtreeStack);
    }
    virtual void visit(ExpressionGetField*) final {
        ensureNotEncryptedEnterEval("a $getField expression", subtreeStack);
    }
    virtual void visit(ExpressionSetField*) final {
        ensureNotEncryptedEnterEval("a $setField expression", subtreeStack);
    }
    virtual void visit(ExpressionToHashedIndexKey*) final {
        ensureNotEncryptedEnterEval("a $hash expression", subtreeStack);
    }
    virtual void visit(ExpressionIfNull*) final {
        // If $ifNull appears under a comparison subtree, then both arguments to $ifNull should be
        // marked or assert just as if they were the direct descendant of the grandparent
        // comparison.
    }
    virtual void visit(ExpressionIn* in);
    virtual void visit(ExpressionIndexOfArray*) final {
        ensureNotEncryptedEnterEval("an array find", subtreeStack);
    }
    virtual void visit(ExpressionIndexOfBytes*) final {
        ensureNotEncryptedEnterEval("a byte-based string find", subtreeStack);
    }
    virtual void visit(ExpressionIndexOfCP*) final {
        ensureNotEncryptedEnterEval("a code-point-based string find", subtreeStack);
    }
    virtual void visit(ExpressionInternalJsEmit*) final {
        ensureNotEncryptedEnterEval("an internal JS emit expression", subtreeStack);
    }
    virtual void visit(ExpressionInternalFindElemMatch*) {
        ensureNotEncryptedEnterEval("an internal find $elemMatch expression", subtreeStack);
    }
    virtual void visit(ExpressionInternalFindPositional*) {
        ensureNotEncryptedEnterEval("an internal find positional expression", subtreeStack);
    }
    virtual void visit(ExpressionInternalFindSlice*) {
        ensureNotEncryptedEnterEval("an internal find $slice expression", subtreeStack);
    }
    virtual void visit(ExpressionIsNumber*) final {
        ensureNotEncryptedEnterEval("a numeric-type checker", subtreeStack);
    }
    virtual void visit(ExpressionLet* let) final {
        for (auto&& [unused, nameAndExpression] : let->getVariableMap())
            if (auto&& [name, unused] = nameAndExpression; name == "CURRENT")
                uasserted(31152, "Rebinding of CURRENT disallowed");
        // It's possible for a $let to have no bindings, so entering a Subtree is conditional on
        // having at least one.
        if (let->getChildren().size() > 1)
            enterSubtree(Subtree::Evaluated{"a let binding"}, subtreeStack);
    }
    virtual void visit(ExpressionLn*) final {
        ensureNotEncryptedEnterEval("a natural logarithm calculation", subtreeStack);
    }
    virtual void visit(ExpressionLog*) final {
        ensureNotEncryptedEnterEval("a logarithm calculation", subtreeStack);
    }
    virtual void visit(ExpressionLog10*) final {
        ensureNotEncryptedEnterEval("a base-ten logarithm calculation", subtreeStack);
    }
    virtual void visit(ExpressionInternalFLEEqual*) final {
        ensureNotEncryptedEnterEval("a fle equal match", subtreeStack);
    }
    virtual void visit(ExpressionInternalFLEBetween*) final {
        ensureNotEncryptedEnterEval("a fle between match", subtreeStack);
    }
    virtual void visit(ExpressionMap*) final {
        ensureNotEncryptedEnterEval("a map function", subtreeStack);
    }
    virtual void visit(ExpressionMeta*) final {
        ensureNotEncrypted("a metadata access", subtreeStack);
    }
    virtual void visit(ExpressionMod*) final {
        ensureNotEncryptedEnterEval("a modulo calculation", subtreeStack);
    }
    virtual void visit(ExpressionMultiply*) final {
        ensureNotEncryptedEnterEval("a multiplication calculation", subtreeStack);
    }
    virtual void visit(ExpressionNot*) final {
        ensureNotEncryptedEnterEval("a negation", subtreeStack);
    }
    virtual void visit(ExpressionObject*) final {
        // Arguably this isn't evaluation but it has the same semantics for now. We could support
        // this with effort.
        ensureNotEncryptedEnterEval("formation of an object literal", subtreeStack);
    }
    virtual void visit(ExpressionOr*) final {
        ensureNotEncryptedEnterEval("a disjunction", subtreeStack);
    }
    virtual void visit(ExpressionPow*) final {
        ensureNotEncryptedEnterEval("an exponentiation calculation", subtreeStack);
    }
    virtual void visit(ExpressionRandom*) final {
        ensureNotEncryptedEnterEval("a $rand expression", subtreeStack);
    }
    virtual void visit(ExpressionRange*) final {
        ensureNotEncryptedEnterEval("a numeric sequence generator", subtreeStack);
    }
    virtual void visit(ExpressionReduce*) final {
        enterSubtree(Subtree::Evaluated{"a reduce initializer"}, subtreeStack);
    }
    virtual void visit(ExpressionReplaceOne*) final {
        ensureNotEncryptedEnterEval("a string replaceOne operation", subtreeStack);
    }
    virtual void visit(ExpressionReplaceAll*) final {
        ensureNotEncryptedEnterEval("a string replaceAll operation", subtreeStack);
    }
    virtual void visit(ExpressionSetDifference*) final {
        ensureNotEncryptedEnterEval("a set difference operation", subtreeStack);
    }
    virtual void visit(ExpressionSetEquals*) final {
        ensureNotEncryptedEnterEval("a set equality operation", subtreeStack);
    }
    virtual void visit(ExpressionSetIntersection*) final {
        ensureNotEncryptedEnterEval("a set intersection operation", subtreeStack);
    }
    virtual void visit(ExpressionSetIsSubset*) final {
        ensureNotEncryptedEnterEval("a subset determination operation", subtreeStack);
    }
    virtual void visit(ExpressionSetUnion*) final {
        ensureNotEncryptedEnterEval("a set union operation", subtreeStack);
    }
    virtual void visit(ExpressionSize*) final {
        ensureNotEncryptedEnterEval("an array size determination", subtreeStack);
    }
    virtual void visit(ExpressionReverseArray*) final {
        ensureNotEncryptedEnterEval("an array reversal", subtreeStack);
    }
    virtual void visit(ExpressionSortArray*) final {
        ensureNotEncryptedEnterEval("an array sorting", subtreeStack);
    }
    virtual void visit(ExpressionSlice*) final {
        ensureNotEncryptedEnterEval("an array subset operation", subtreeStack);
    }
    virtual void visit(ExpressionIsArray*) final {
        ensureNotEncryptedEnterEval("an array type determination", subtreeStack);
    }
    virtual void visit(ExpressionInternalFindAllValuesAtPath*) final {
        ensureNotEncryptedEnterEval("an array deep unwinding operation", subtreeStack);
    }
    virtual void visit(ExpressionRound*) final {
        ensureNotEncryptedEnterEval("a rounding calculation", subtreeStack);
    }
    virtual void visit(ExpressionSplit*) final {
        ensureNotEncryptedEnterEval("a string split", subtreeStack);
    }
    virtual void visit(ExpressionSqrt*) final {
        ensureNotEncryptedEnterEval("a square root calculation", subtreeStack);
    }
    virtual void visit(ExpressionStrcasecmp*) final {
        ensureNotEncryptedEnterEval("a case-insensitive string comparison", subtreeStack);
    }
    virtual void visit(ExpressionSubstrBytes*) final {
        ensureNotEncryptedEnterEval("a byte-based substring operation", subtreeStack);
    }
    virtual void visit(ExpressionSubstrCP*) final {
        ensureNotEncryptedEnterEval("a code-point-based substring operation", subtreeStack);
    }
    virtual void visit(ExpressionStrLenBytes*) final {
        ensureNotEncryptedEnterEval("a byte-based string length determination", subtreeStack);
    }
    virtual void visit(ExpressionBinarySize*) final {
        ensureNotEncryptedEnterEval("a byte-based string or BinData length determination",
                                    subtreeStack);
    }
    virtual void visit(ExpressionStrLenCP*) final {
        ensureNotEncryptedEnterEval("a code-point-based string length determination", subtreeStack);
    }
    virtual void visit(ExpressionSubtract*) final {
        ensureNotEncryptedEnterEval("a subtraction calculation", subtreeStack);
    }
    virtual void visit(ExpressionSwitch*) final {
        // We need to enter an Evaluated output Subtree for each case child.
        enterSubtree(Subtree::Evaluated{"a switch case"}, subtreeStack);
    }
    virtual void visit(ExpressionTestApiVersion*) final {
        enterSubtree(Subtree::Evaluated{"an API version evaluation"}, subtreeStack);
    }
    virtual void visit(ExpressionToLower*) final {
        ensureNotEncryptedEnterEval("a string lowercase conversion", subtreeStack);
    }
    virtual void visit(ExpressionToUpper*) final {
        ensureNotEncryptedEnterEval("a string uppercase conversion", subtreeStack);
    }
    virtual void visit(ExpressionTrim*) final {
        ensureNotEncryptedEnterEval("a string trim operation", subtreeStack);
    }
    virtual void visit(ExpressionTrunc*) final {
        ensureNotEncryptedEnterEval("a string truncation operation", subtreeStack);
    }
    virtual void visit(ExpressionType*) final {
        ensureNotEncryptedEnterEval("a string type determination", subtreeStack);
    }
    virtual void visit(ExpressionZip*) final {
        ensureNotEncryptedEnterEval("an array zip operation", subtreeStack);
    }
    virtual void visit(ExpressionConvert*) final {
        ensureNotEncryptedEnterEval("a type conversion", subtreeStack);
    }
    virtual void visit(ExpressionRegexFind*) final {
        ensureNotEncryptedEnterEval("a regex find operation", subtreeStack);
    }
    virtual void visit(ExpressionRegexFindAll*) final {
        ensureNotEncryptedEnterEval("a regex find all operation", subtreeStack);
    }
    virtual void visit(ExpressionRegexMatch*) final {
        ensureNotEncryptedEnterEval("a regex match operation", subtreeStack);
    }
    virtual void visit(ExpressionCosine*) final {
        ensureNotEncryptedEnterEval("a cosine calculation", subtreeStack);
    }
    virtual void visit(ExpressionSine*) final {
        ensureNotEncryptedEnterEval("a sine calculation", subtreeStack);
    }
    virtual void visit(ExpressionTangent*) final {
        ensureNotEncryptedEnterEval("a tangent calculation", subtreeStack);
    }
    virtual void visit(ExpressionArcCosine*) final {
        ensureNotEncryptedEnterEval("an inverse cosine calculation", subtreeStack);
    }
    virtual void visit(ExpressionArcSine*) final {
        ensureNotEncryptedEnterEval("an inverse sine calculation", subtreeStack);
    }
    virtual void visit(ExpressionArcTangent*) final {
        ensureNotEncryptedEnterEval("an inverse tangent calculation", subtreeStack);
    }
    virtual void visit(ExpressionArcTangent2*) final {
        ensureNotEncryptedEnterEval("an inverse tangent calculation", subtreeStack);
    }
    virtual void visit(ExpressionHyperbolicArcTangent*) final {
        ensureNotEncryptedEnterEval("a hyperbolic inverse tangent calculation", subtreeStack);
    }
    virtual void visit(ExpressionHyperbolicArcCosine*) final {
        ensureNotEncryptedEnterEval("a hyperbolic inverse cosine calculation", subtreeStack);
    }
    virtual void visit(ExpressionHyperbolicArcSine*) final {
        ensureNotEncryptedEnterEval("a hyperbolic inverse sine calculation", subtreeStack);
    }
    virtual void visit(ExpressionHyperbolicTangent*) final {
        ensureNotEncryptedEnterEval("a hyperbolic tangent calculation", subtreeStack);
    }
    virtual void visit(ExpressionHyperbolicCosine*) final {
        ensureNotEncryptedEnterEval("a hyperbolic cosine calculation", subtreeStack);
    }
    virtual void visit(ExpressionHyperbolicSine*) final {
        ensureNotEncryptedEnterEval("a hyperbolic sine calculation", subtreeStack);
    }
    virtual void visit(ExpressionDegreesToRadians*) final {
        ensureNotEncryptedEnterEval("a degree to radian conversion", subtreeStack);
    }
    virtual void visit(ExpressionRadiansToDegrees*) final {
        ensureNotEncryptedEnterEval("a radian to degree conversion", subtreeStack);
    }
    virtual void visit(ExpressionDayOfMonth*) final {
        ensureNotEncryptedEnterEval("a day of month extractor", subtreeStack);
    }
    virtual void visit(ExpressionDayOfWeek*) final {
        ensureNotEncryptedEnterEval("a day of week extractor", subtreeStack);
    }
    virtual void visit(ExpressionDayOfYear*) final {
        ensureNotEncryptedEnterEval("a day of year extractor", subtreeStack);
    }
    virtual void visit(ExpressionHour*) final {
        ensureNotEncryptedEnterEval("an hour extractor", subtreeStack);
    }
    virtual void visit(ExpressionMillisecond*) final {
        ensureNotEncryptedEnterEval("a millisecond extractor", subtreeStack);
    }
    virtual void visit(ExpressionMinute*) final {
        ensureNotEncryptedEnterEval("a minute extractor", subtreeStack);
    }
    virtual void visit(ExpressionMonth*) final {
        ensureNotEncryptedEnterEval("a month extractor", subtreeStack);
    }
    virtual void visit(ExpressionSecond*) final {
        ensureNotEncryptedEnterEval("a second extractor", subtreeStack);
    }
    virtual void visit(ExpressionWeek*) final {
        ensureNotEncryptedEnterEval("a week of year extractor", subtreeStack);
    }
    virtual void visit(ExpressionIsoWeekYear*) final {
        ensureNotEncryptedEnterEval("an ISO 8601 week and year extractor", subtreeStack);
    }
    virtual void visit(ExpressionIsoDayOfWeek*) final {
        ensureNotEncryptedEnterEval("an ISO 8601 day of week extractor", subtreeStack);
    }
    virtual void visit(ExpressionIsoWeek*) final {
        ensureNotEncryptedEnterEval("an ISO 8601 week extractor", subtreeStack);
    }
    virtual void visit(ExpressionYear*) final {
        ensureNotEncryptedEnterEval("an ISO 8601 year extractor", subtreeStack);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorAvg>*) final {
        ensureNotEncryptedEnterEval("an average aggregation", subtreeStack);
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorFirstN>*) final {
        ensureNotEncryptedEnterEval("an aggregation of the first 'n' values", subtreeStack);
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorLastN>*) final {
        ensureNotEncryptedEnterEval("an aggregation of the last 'n' values", subtreeStack);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorMax>*) final {
        ensureNotEncryptedEnterEval("a maximum aggregation", subtreeStack);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorMin>*) final {
        ensureNotEncryptedEnterEval("a minimum aggregation", subtreeStack);
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorMaxN>*) final {
        ensureNotEncryptedEnterEval("a maximum aggregation of up to 'n' values", subtreeStack);
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorMinN>*) final {
        ensureNotEncryptedEnterEval("a minimum aggregation of up to 'n' values", subtreeStack);
    }
    virtual void visit(ExpressionFromAccumulatorQuantile<AccumulatorMedian>*) final {
        ensureNotEncryptedEnterEval("a percentile calculation", subtreeStack);
    }
    virtual void visit(ExpressionFromAccumulatorQuantile<AccumulatorPercentile>*) final {
        ensureNotEncryptedEnterEval("a percentile calculation", subtreeStack);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>*) final {
        ensureNotEncryptedEnterEval("a population standard deviation aggregation", subtreeStack);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>*) final {
        ensureNotEncryptedEnterEval("a sample standard deviation aggregation", subtreeStack);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorSum>*) final {
        ensureNotEncryptedEnterEval("a sum aggregation", subtreeStack);
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>*) final {
        ensureNotEncryptedEnterEval("a merge objects aggregation", subtreeStack);
    }
    virtual void visit(ExpressionTsSecond*) final {
        ensureNotEncryptedEnterEval("a timestamp second component extractor", subtreeStack);
    }
    virtual void visit(ExpressionTsIncrement*) final {
        ensureNotEncryptedEnterEval("a timestamp increment component extractor", subtreeStack);
    }

    virtual void visit(ExpressionTests::Testable*) final {}
    virtual void visit(ExpressionInternalOwningShard*) final {
        ensureNotEncryptedEnterEval("a shard id computing operation", subtreeStack);
    }
    virtual void visit(ExpressionInternalIndexKey*) final {
        ensureNotEncryptedEnterEval("an index keys objects generation computing operation",
                                    subtreeStack);
    }
    virtual void visit(ExpressionInternalKeyStringValue*) final {
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
    virtual void visit(ExpressionConstant*) {}
    virtual void visit(ExpressionAbs*) {}
    virtual void visit(ExpressionAdd*) {}
    virtual void visit(ExpressionAllElementsTrue*) {}
    virtual void visit(ExpressionAnd*) {}
    virtual void visit(ExpressionAnyElementTrue*) {}
    virtual void visit(ExpressionArray*) {}
    virtual void visit(ExpressionArrayElemAt*) {}
    virtual void visit(ExpressionBitAnd*) {}
    virtual void visit(ExpressionBitOr*) {}
    virtual void visit(ExpressionBitXor*) {}
    virtual void visit(ExpressionBitNot*) {}
    virtual void visit(ExpressionFirst*) {}
    virtual void visit(ExpressionLast*) {}
    virtual void visit(ExpressionObjectToArray*) {}
    virtual void visit(ExpressionArrayToObject*) {}
    virtual void visit(ExpressionBsonSize*) {}
    virtual void visit(ExpressionCeil*) {}
    virtual void visit(ExpressionCoerceToBool*) {}
    virtual void visit(ExpressionCompare*) {}
    virtual void visit(ExpressionConcat*) {}
    virtual void visit(ExpressionConcatArrays*) {}
    virtual void visit(ExpressionCond*) {
        if (numChildrenVisited == 1ull)
            // We need to exit the Evaluated output Subtree for if child.
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
        // The then and else children should be part of the parent Subtree.
    }
    virtual void visit(ExpressionDateAdd*) {}
    virtual void visit(ExpressionDateDiff*) {}
    virtual void visit(ExpressionDateFromString*) {}
    virtual void visit(ExpressionDateFromParts*) {}
    virtual void visit(ExpressionDateSubtract*) {}
    virtual void visit(ExpressionDateToParts*) {}
    virtual void visit(ExpressionDateToString*) {}
    virtual void visit(ExpressionDateTrunc*) {}
    virtual void visit(ExpressionDivide*) {}
    virtual void visit(ExpressionExp*) {}
    virtual void visit(ExpressionFieldPath*) {}
    virtual void visit(ExpressionFilter*) {}
    virtual void visit(ExpressionFloor*) {}
    virtual void visit(ExpressionFunction*) {}
    virtual void visit(ExpressionGetField*) {}
    virtual void visit(ExpressionSetField*) {}
    virtual void visit(ExpressionTestApiVersion*) {}
    virtual void visit(ExpressionToHashedIndexKey*) {}
    virtual void visit(ExpressionIfNull*) {}
    virtual void visit(ExpressionIn* in) {
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
    virtual void visit(ExpressionIndexOfArray*) {}
    virtual void visit(ExpressionIndexOfBytes*) {}
    virtual void visit(ExpressionIndexOfCP*) {}
    virtual void visit(ExpressionInternalJsEmit*) {}
    virtual void visit(ExpressionInternalFindElemMatch*) {}
    virtual void visit(ExpressionInternalFindPositional*) {}
    virtual void visit(ExpressionInternalFindSlice*) {}
    virtual void visit(ExpressionIsNumber*) {}
    virtual void visit(ExpressionLet* let) final {
        // The final child of a let Expression is part of the parent Subtree.
        if (numChildrenVisited == let->getChildren().size() - 1)
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionLn*) {}
    virtual void visit(ExpressionLog*) {}
    virtual void visit(ExpressionLog10*) {}
    virtual void visit(ExpressionInternalFLEEqual*) {}
    virtual void visit(ExpressionInternalFLEBetween*) {}
    virtual void visit(ExpressionMap*) {}
    virtual void visit(ExpressionMeta*) {}
    virtual void visit(ExpressionMod*) {}
    virtual void visit(ExpressionMultiply*) {}
    virtual void visit(ExpressionNot*) {}
    virtual void visit(ExpressionObject*) {}
    virtual void visit(ExpressionOr*) {}
    virtual void visit(ExpressionPow*) {}
    virtual void visit(ExpressionRandom*) {}
    virtual void visit(ExpressionRange*) {}
    virtual void visit(ExpressionReduce* reduce) {
        // As with ExpressionLet the child here is part of the parent Subtree.
        if (numChildrenVisited == reduce->getChildren().size() - 1)
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionReplaceOne*) {}
    virtual void visit(ExpressionReplaceAll*) {}
    virtual void visit(ExpressionSetDifference*) {}
    virtual void visit(ExpressionSetEquals*) {}
    virtual void visit(ExpressionSetIntersection*) {}
    virtual void visit(ExpressionSetIsSubset*) {}
    virtual void visit(ExpressionSetUnion*) {}
    virtual void visit(ExpressionSize*) {}
    virtual void visit(ExpressionReverseArray*) {}
    virtual void visit(ExpressionSortArray*) {}
    virtual void visit(ExpressionSlice*) {}
    virtual void visit(ExpressionIsArray*) {}
    virtual void visit(ExpressionInternalFindAllValuesAtPath*) {}
    virtual void visit(ExpressionRound*) {}
    virtual void visit(ExpressionSplit*) {}
    virtual void visit(ExpressionSqrt*) {}
    virtual void visit(ExpressionStrcasecmp*) {}
    virtual void visit(ExpressionSubstrBytes*) {}
    virtual void visit(ExpressionSubstrCP*) {}
    virtual void visit(ExpressionStrLenBytes*) {}
    virtual void visit(ExpressionBinarySize*) {}
    virtual void visit(ExpressionStrLenCP*) {}
    virtual void visit(ExpressionSubtract*) {}
    virtual void visit(ExpressionSwitch* switchExpr) {
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
    virtual void visit(ExpressionToLower*) {}
    virtual void visit(ExpressionToUpper*) {}
    virtual void visit(ExpressionTrim*) {}
    virtual void visit(ExpressionTrunc*) {}
    virtual void visit(ExpressionType*) {}
    virtual void visit(ExpressionZip*) {}
    virtual void visit(ExpressionConvert*) {}
    virtual void visit(ExpressionRegexFind*) {}
    virtual void visit(ExpressionRegexFindAll*) {}
    virtual void visit(ExpressionRegexMatch*) {}
    virtual void visit(ExpressionCosine*) {}
    virtual void visit(ExpressionSine*) {}
    virtual void visit(ExpressionTangent*) {}
    virtual void visit(ExpressionArcCosine*) {}
    virtual void visit(ExpressionArcSine*) {}
    virtual void visit(ExpressionArcTangent*) {}
    virtual void visit(ExpressionArcTangent2*) {}
    virtual void visit(ExpressionHyperbolicArcTangent*) {}
    virtual void visit(ExpressionHyperbolicArcCosine*) {}
    virtual void visit(ExpressionHyperbolicArcSine*) {}
    virtual void visit(ExpressionHyperbolicTangent*) {}
    virtual void visit(ExpressionHyperbolicCosine*) {}
    virtual void visit(ExpressionHyperbolicSine*) {}
    virtual void visit(ExpressionDegreesToRadians*) {}
    virtual void visit(ExpressionRadiansToDegrees*) {}
    virtual void visit(ExpressionDayOfMonth*) {}
    virtual void visit(ExpressionDayOfWeek*) {}
    virtual void visit(ExpressionDayOfYear*) {}
    virtual void visit(ExpressionHour*) {}
    virtual void visit(ExpressionMillisecond*) {}
    virtual void visit(ExpressionMinute*) {}
    virtual void visit(ExpressionMonth*) {}
    virtual void visit(ExpressionSecond*) {}
    virtual void visit(ExpressionWeek*) {}
    virtual void visit(ExpressionIsoWeekYear*) {}
    virtual void visit(ExpressionIsoDayOfWeek*) {}
    virtual void visit(ExpressionIsoWeek*) {}
    virtual void visit(ExpressionYear*) {}
    virtual void visit(ExpressionFromAccumulator<AccumulatorAvg>*) {}
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorFirstN>*) {}
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorLastN>*) {}
    virtual void visit(ExpressionFromAccumulator<AccumulatorMax>*) {}
    virtual void visit(ExpressionFromAccumulator<AccumulatorMin>*) {}
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorMaxN>*) {}
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorMinN>*) {}
    virtual void visit(ExpressionFromAccumulatorQuantile<AccumulatorMedian>*) {}
    virtual void visit(ExpressionFromAccumulatorQuantile<AccumulatorPercentile>*) {}
    virtual void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>*) {}
    virtual void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>*) {}
    virtual void visit(ExpressionFromAccumulator<AccumulatorSum>*) {}
    virtual void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>*) {}
    virtual void visit(ExpressionTsSecond*) {}
    virtual void visit(ExpressionTsIncrement*) {}
    virtual void visit(ExpressionTests::Testable*) {}
    virtual void visit(ExpressionInternalOwningShard*) {}
    virtual void visit(ExpressionInternalIndexKey*) {}
    virtual void visit(ExpressionInternalKeyStringValue*) {}


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
    virtual void visit(ExpressionConstant*) final {}
    virtual void visit(ExpressionAbs*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionAdd*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionAllElementsTrue*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionAnd*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionAnyElementTrue*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionArray*) {
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
    virtual void visit(ExpressionArrayElemAt*) {
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
    virtual void visit(ExpressionFirst*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionLast*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionObjectToArray*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionArrayToObject*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionBitNot*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionBsonSize*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionCeil*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionCoerceToBool*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionCompare* compare) {
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
    virtual void visit(ExpressionConcat*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionConcatArrays*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionCond*) {}
    virtual void visit(ExpressionDateAdd*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionDateDiff*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionDateFromParts*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionDateFromString*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionDateSubtract*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionDateToParts*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionDateToString*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionDateTrunc*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionDivide*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionExp*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFieldPath*) {}
    virtual void visit(ExpressionFilter*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFloor*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFunction*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionGetField*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSetField*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionTestApiVersion*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionToHashedIndexKey*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionIfNull*) {}
    virtual void visit(ExpressionIn* in) {
        // See the comment in the PreVisitor about why we have to special case an array literal.
        if (dynamic_cast<ExpressionArray*>(in->getOperandList()[1].get())) {
            didSetIntention =
                exitSubtree<Subtree::Compared>(expCtx, subtreeStack) || didSetIntention;
        } else {
            didSetIntention =
                exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
        }
    }
    virtual void visit(ExpressionIndexOfArray*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionIndexOfBytes*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionIsNumber*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionIndexOfCP*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionInternalJsEmit*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionInternalFindElemMatch*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionInternalFindPositional*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionInternalFindSlice*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionLet*) {}
    virtual void visit(ExpressionLn*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionLog*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionLog10*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionInternalFLEEqual*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionInternalFLEBetween*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionMap*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionMeta*) {}
    virtual void visit(ExpressionMod*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionMultiply*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionNot*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionObject*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionOr*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionPow*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionRandom*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionRange*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionReduce*) {}
    virtual void visit(ExpressionReplaceOne*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionReplaceAll*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSetDifference*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSetEquals*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSetIntersection*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSetIsSubset*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSetUnion*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSize*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionReverseArray*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSortArray*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSlice*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionIsArray*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionInternalFindAllValuesAtPath*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionRound*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSplit*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSqrt*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionStrcasecmp*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSubstrBytes*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSubstrCP*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionStrLenBytes*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionBinarySize*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionStrLenCP*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSubtract*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSwitch*) {
        // We are exiting the default branch which is part of the parent Subtree so no work is
        // required here.
    }
    virtual void visit(ExpressionToLower*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionToUpper*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionTrim*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionTrunc*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionType*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionZip*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionConvert*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionRegexFind*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionRegexFindAll*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionRegexMatch*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionCosine*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSine*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionTangent*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionArcCosine*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionArcSine*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionArcTangent*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionArcTangent2*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionHyperbolicArcTangent*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionHyperbolicArcCosine*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionHyperbolicArcSine*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionHyperbolicTangent*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionHyperbolicCosine*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionHyperbolicSine*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionDegreesToRadians*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionRadiansToDegrees*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionDayOfMonth*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionDayOfWeek*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionDayOfYear*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionHour*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionMillisecond*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionMinute*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionMonth*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionSecond*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionWeek*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionIsoWeekYear*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionIsoDayOfWeek*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionIsoWeek*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionYear*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorAvg>*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorFirstN>*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorLastN>*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorMax>*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorMin>*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorMaxN>*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFromAccumulatorN<AccumulatorMinN>*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFromAccumulatorQuantile<AccumulatorMedian>*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFromAccumulatorQuantile<AccumulatorPercentile>*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorSum>*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionTsSecond*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionTsIncrement*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionInternalOwningShard*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionInternalIndexKey*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }
    virtual void visit(ExpressionInternalKeyStringValue*) {
        didSetIntention = exitSubtree<Subtree::Evaluated>(expCtx, subtreeStack) || didSetIntention;
    }


    virtual void visit(ExpressionTests::Testable*) {}

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
