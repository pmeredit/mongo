/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */


#include "mongo/platform/basic.h"

#include "aggregate_expression_intender_entry.h"
#include "fle_match_expression.h"

#include "mongo/crypto/encryption_fields_gen.h"
#include "mongo/crypto/fle_field_schema_gen.h"
#include "mongo/db/matcher/expression_expr.h"
#include "mongo/db/matcher/expression_leaf.h"
#include "mongo/db/matcher/schema/expression_internal_schema_eq.h"
#include "mongo/db/query/index_bounds.h"
#include "mongo/db/query/index_bounds_builder.h"
#include "mongo/db/query/index_entry.h"
#include "query_analysis.h"
#include <limits>

namespace mongo {

using MatchType = MatchExpression::MatchType;
using EncryptionPlaceholderContext = query_analysis::EncryptionPlaceholderContext;

FLEMatchExpression::FLEMatchExpression(std::unique_ptr<MatchExpression> expression,
                                       const EncryptionSchemaTreeNode& schemaTree,
                                       FLE2FieldRefExpr fieldRefSupported)
    : _expression(std::move(expression)), fieldRefSupported(fieldRefSupported) {
    // Only perform query analysis for queries more advanced than equality if working with FLE2.
    if (schemaTree.parsedFrom == FleVersion::kFle2 &&
        gFeatureFlagFLE2Range.isEnabled(serverGlobalParams.featureCompatibility)) {
        // The range index rewrite might replace the top-level expression (e.g. $gt ->
        // $between), and so may allocate a new root node. If it does, swap out the old
        // expression.
        if (auto rangeReplaced = replaceEncryptedRangeElements(schemaTree, _expression.get())) {
            _expression.swap(rangeReplaced);
        }
    }
    replaceEncryptedEqualityElements(schemaTree, _expression.get());
}

BSONElement FLEMatchExpression::allocateEncryptedEqualityElement(
    const BSONElement& elem,
    const ResolvedEncryptionInfo& metadata,
    const CollatorInterface* collator) {
    _encryptedElements.push_back(buildEncryptPlaceholder(
        elem, metadata, EncryptionPlaceholderContext::kComparison, collator));
    _didMark = aggregate_expression_intender::Intention::Marked;
    return _encryptedElements.back().firstElement();
}

void FLEMatchExpression::replaceEqualityElementsInEqExpression(
    const EncryptionSchemaTreeNode& schemaTree, EqualityMatchExpression* eqExpr) {
    if (auto encryptMetadata = schemaTree.getEncryptionMetadataForPath(FieldRef(eqExpr->path()))) {
        // Queries involving comparisons to null cannot work with encryption, as the expected
        // semantics involve returning documents where the encrypted field is missing, null, or
        // undefined. Building an encryption placeholder with a null element will only return
        // documents with the literal null, not missing or undefined.
        uassert(51095,
                str::stream() << "Illegal equality to null predicate for encrypted field: '"
                              << eqExpr->path() << "'",
                !eqExpr->getData().isNull());

        eqExpr->setData(allocateEncryptedEqualityElement(
            eqExpr->getData(), encryptMetadata.value(), eqExpr->getCollator()));
    } else {
        // The path to the $eq expression is not encrypted, however there may still be an encrypted
        // field within the RHS object.
        auto rhsElem = eqExpr->getData();
        if (rhsElem.type() == BSONType::Object) {
            auto [hasEncrypt, unused, unused2, placeholder] =
                replaceEncryptedFields(rhsElem.embeddedObject(),
                                       &schemaTree,
                                       EncryptionPlaceholderContext::kComparison,
                                       FieldRef(eqExpr->path()),
                                       boost::none,
                                       eqExpr->getCollator());
            if (hasEncrypt) {
                uassert(6341900,
                        "Comparisons to objects which contain encrypted payloads is not allowed "
                        "with Queryable Encryption",
                        schemaTree.parsedFrom != FleVersion::kFle2);
                eqExpr->setData(allocateEncryptedObject(placeholder));
            }
        } else if (rhsElem.type() == BSONType::Array) {
            uassert(
                31007,
                str::stream() << "$eq to array predicate is illegal for prefix of encrypted path: "
                              << eqExpr->toString(),
                !schemaTree.mayContainEncryptedNodeBelowPrefix(FieldRef{eqExpr->path()}));
        }
    }
}

void FLEMatchExpression::replaceEqualityElementsInInExpression(
    const EncryptionSchemaTreeNode& schemaTree, InMatchExpression* inExpr) {
    std::vector<BSONElement> replacedElements;
    if (auto encryptMetadata = schemaTree.getEncryptionMetadataForPath(FieldRef(inExpr->path()))) {
        uassert(51015,
                str::stream() << "Illegal regex inside $in against an encrypted field: '"
                              << inExpr->path() << "'",
                inExpr->getRegexes().empty());

        // Replace each element in the $in expression with its encryption placeholder.
        for (auto&& elem : inExpr->getEqualities()) {
            uassert(
                51120,
                str::stream() << "Illegal equality to null inside $in against an encrypted field: '"
                              << inExpr->path() << "'",
                !elem.isNull());
            replacedElements.push_back(allocateEncryptedEqualityElement(
                elem, encryptMetadata.value(), inExpr->getCollator()));
        }
    } else {
        // The path to the $in expression is not encrypted, however there may still be an
        // encrypted field within any RHS objects of the $in array.
        bool hasPlaceholders = false;
        for (auto&& elem : inExpr->getEqualities()) {
            if (elem.type() == BSONType::Object) {
                auto [elemHasEncrypt, unused, unused2, placeholder] =
                    replaceEncryptedFields(elem.embeddedObject(),
                                           &schemaTree,
                                           EncryptionPlaceholderContext::kComparison,
                                           FieldRef(inExpr->path()),
                                           boost::none,
                                           inExpr->getCollator());

                // This class maintains an invariant that BSON storage is allocated if and only if
                // the underlying MatchExpression has been marked with at least one
                // intent-to-encrypt placeholder.
                if (elemHasEncrypt) {
                    uassert(
                        6341901,
                        "Comparisons to objects which contain encrypted payloads is not allowed "
                        "with Queryable Encryption",
                        schemaTree.parsedFrom != FleVersion::kFle2);
                    replacedElements.push_back(allocateEncryptedObject(placeholder));
                } else {
                    replacedElements.push_back(elem);
                }

                hasPlaceholders = hasPlaceholders || elemHasEncrypt;
            } else if (elem.type() == BSONType::Array) {
                uassert(31008,
                        str::stream() << "Illegal $in element for prefix '" << inExpr->path()
                                      << "' of encrypted path: " << elem,
                        !schemaTree.mayContainEncryptedNodeBelowPrefix(FieldRef{inExpr->path()}));
                replacedElements.push_back(elem);
            } else {
                replacedElements.push_back(elem);
            }
        }
    }

    uassertStatusOK(inExpr->setEqualities(std::move(replacedElements)));
}

void FLEMatchExpression::replaceEncryptedEqualityElements(
    const EncryptionSchemaTreeNode& schemaTree, MatchExpression* root) {
    invariant(root);

    switch (root->matchType()) {
        // Allowlist of expressions which are allowed on encrypted fields.
        case MatchType::EQ: {
            replaceEqualityElementsInEqExpression(schemaTree,
                                                  static_cast<EqualityMatchExpression*>(root));
            break;
        }

        case MatchType::EXPRESSION: {
            auto expr = static_cast<ExprMatchExpression*>(root);
            _didMark = aggregate_expression_intender::mark(expr->getExpressionContext().get(),
                                                           schemaTree,
                                                           expr->getExpressionRef(),
                                                           false,
                                                           fieldRefSupported);
            break;
        }

        case MatchType::MATCH_IN: {
            replaceEqualityElementsInInExpression(schemaTree,
                                                  static_cast<InMatchExpression*>(root));
            break;
        }

        // Expressions which contain one or more children, fall through to recurse on each child.
        case MatchType::AND:
        case MatchType::INTERNAL_SCHEMA_COND:
        case MatchType::OR:
        case MatchType::NOT:
        case MatchType::NOR:
        case MatchType::INTERNAL_SCHEMA_XOR:
            break;

        // These expressions could contain constants which need to be marked for encryption, but the
        // FLE query analyzer does not understand them. Error unconditionally if we encounter any of
        // the node types in this list.
        case MatchType::INTERNAL_SCHEMA_ALLOWED_PROPERTIES:
        case MatchType::INTERNAL_SCHEMA_MAX_PROPERTIES:
        case MatchType::INTERNAL_SCHEMA_MIN_PROPERTIES:
        case MatchType::INTERNAL_SCHEMA_OBJECT_MATCH:
        case MatchType::INTERNAL_SCHEMA_ROOT_DOC_EQ:
        case MatchType::TEXT:
        case MatchType::WHERE:
            uasserted(51094,
                      str::stream() << "Unsupported match expression operator for encryption: "
                                    << root->toString());

        // The children expressions of $elemMatch predicates are parsed as top-level expressions,
        // without the path prefix of the enclosing array field. Query analysis should not recurse
        // into $elemMatch children, because they will be treated as top-level fields rather than
        // fields within subdocuments inside an array. This is safe to do because the contents of
        // arrays cannot be encrypted currently.
        // TODO: SERVER-69377 support $elemMatch in query analysis.
        case MatchType::ELEM_MATCH_OBJECT:
        case MatchType::ELEM_MATCH_VALUE:
            uassert(6890100,
                    str::stream() << "$elemMatch is unsupported on encrypted fields: "
                                  << root->toString(),
                    !schemaTree.getEncryptionMetadataForPath(FieldRef(root->path())));
            return;  // return early to skip recursion into children nodes.

        // Leaf expressions which are not allowed to operate on encrypted fields. Some of these
        // expressions may not contain sensitive data but the query itself does not make sense on an
        // encrypted field.
        case MatchType::BITS_ALL_SET:
        case MatchType::BITS_ALL_CLEAR:
        case MatchType::BITS_ANY_SET:
        case MatchType::BITS_ANY_CLEAR:
        case MatchType::GEO:
        case MatchType::GEO_NEAR:
        case MatchType::INTERNAL_BUCKET_GEO_WITHIN:
        case MatchType::INTERNAL_2D_POINT_IN_ANNULUS:
        case MatchType::INTERNAL_EXPR_EQ:
        case MatchType::INTERNAL_EXPR_GT:
        case MatchType::INTERNAL_EXPR_GTE:
        case MatchType::INTERNAL_EXPR_LT:
        case MatchType::INTERNAL_EXPR_LTE:
        case MatchType::INTERNAL_SCHEMA_ALL_ELEM_MATCH_FROM_INDEX:
        case MatchType::INTERNAL_SCHEMA_BIN_DATA_ENCRYPTED_TYPE:
        case MatchType::INTERNAL_SCHEMA_BIN_DATA_FLE2_ENCRYPTED_TYPE:
        case MatchType::INTERNAL_SCHEMA_BIN_DATA_SUBTYPE:
        case MatchType::INTERNAL_SCHEMA_EQ:
        case MatchType::INTERNAL_SCHEMA_FMOD:
        case MatchType::INTERNAL_SCHEMA_MATCH_ARRAY_INDEX:
        case MatchType::INTERNAL_SCHEMA_MAX_ITEMS:
        case MatchType::INTERNAL_SCHEMA_MAX_LENGTH:
        case MatchType::INTERNAL_SCHEMA_MIN_LENGTH:
        case MatchType::INTERNAL_SCHEMA_MIN_ITEMS:
        case MatchType::INTERNAL_SCHEMA_TYPE:
        case MatchType::INTERNAL_SCHEMA_UNIQUE_ITEMS:
        case MatchType::MOD:
        case MatchType::REGEX:
        case MatchType::SIZE:
        case MatchType::TYPE_OPERATOR:
            uassert(51092,
                    str::stream() << "Invalid match expression operator on encrypted field '"
                                  << root->path() << "': " << root->toString(),
                    !schemaTree.getEncryptionMetadataForPath(FieldRef(root->path())));
            break;
        case MatchType::BETWEEN: {
            uassert(6721002,
                    "$between is not supported with CSFLE.",
                    schemaTree.parsedFrom == FleVersion::kFle2);
            auto metadata = schemaTree.getEncryptionMetadataForPath(FieldRef(root->path()));
            // Once it's been established that we're processing a FLE2 query, we can assert that
            // $between must have been inserted by a previous analysis pass and therefore
            // satisfies the following invariants.
            tassert(6721003,
                    "$between must be used with a range index.",
                    metadata && metadata->algorithmIs(Fle2AlgorithmInt::kRange));
            auto betweenExpr = static_cast<BetweenMatchExpression*>(root);
            auto payload = betweenExpr->rhs();
            invariant(payload.isBinData(BinDataType::Encrypt));
            break;
        }
        case MatchType::GT:
        case MatchType::GTE:
        case MatchType::LTE:
        case MatchType::LT: {
            uassert(51118,
                    str::stream() << "Invalid match expression operator on encrypted field '"
                                  << root->path() << "': " << root->toString(),
                    !schemaTree.getEncryptionMetadataForPath(FieldRef(root->path())));
            // For comparison match expressions, also reject encrypted fields within RHS objects of
            // the expression.
            auto compExpr = static_cast<ComparisonMatchExpression*>(root);
            auto rhsElem = compExpr->getData();
            if (rhsElem.type() == BSONType::Object) {
                auto [hasEncrypt, unused, unused2, placeholder] =
                    replaceEncryptedFields(rhsElem.embeddedObject(),
                                           &schemaTree,
                                           EncryptionPlaceholderContext::kComparison,
                                           FieldRef(compExpr->path()),
                                           boost::none,
                                           compExpr->getCollator());
                uassert(51119,
                        str::stream() << "Invalid match expression operator on encrypted field '"
                                      << root->toString() << "'",
                        !hasEncrypt);
            }
            break;
        }

        // These expressions cannot contain constants that need to be marked for encryption, and are
        // safe to run regardless of the encryption schema.
        case MatchType::ALWAYS_FALSE:
        case MatchType::ALWAYS_TRUE:
        case MatchType::EXISTS:
            break;
    }

    // Recursively descend each child of this expression.
    for (size_t index = 0; index < root->numChildren(); ++index) {
        replaceEncryptedEqualityElements(schemaTree, root->getChild(index));
    }
}

namespace {

/**
 * Allocate a new $between MatchExpression for the given comparison operation. Because
 * single comparison operations can't represent closed ranges with two finite endpoints, the ranges
 * with the encrypted placeholder will have -inf or inf as the other endpoint.
 */
std::unique_ptr<MatchExpression> makeOneSidedEncryptedBetween(
    const ResolvedEncryptionInfo& metadata, const ComparisonMatchExpression* comp) {
    uassert(6747900,
            str::stream() << "Range query predicate must be within the bounds of encrypted index.",
            literalWithinRangeBounds(metadata, comp->getData()));
    auto ki = metadata.keyId.uuids()[0];
    // TODO: SERVER-67421 support multiple queries for a field.
    auto indexConfig = metadata.fle2SupportedQueries.get()[0];

    auto endpoint = comp->getData();
    auto min = BSON("" << -std::numeric_limits<double>::infinity());
    auto max = BSON("" << std::numeric_limits<double>::infinity());
    switch (comp->matchType()) {
        case MatchExpression::LTE:
            return buildEncryptedBetweenWithPlaceholder(
                comp->path(), ki, indexConfig, {min.firstElement(), true}, {endpoint, true});
        case MatchExpression::LT:
            return buildEncryptedBetweenWithPlaceholder(
                comp->path(), ki, indexConfig, {min.firstElement(), true}, {endpoint, false});
        case MatchExpression::GTE:
            return buildEncryptedBetweenWithPlaceholder(
                comp->path(), ki, indexConfig, {endpoint, true}, {max.firstElement(), true});
        case MatchExpression::GT:
            return buildEncryptedBetweenWithPlaceholder(
                comp->path(), ki, indexConfig, {endpoint, false}, {max.firstElement(), true});
        default:
            MONGO_UNREACHABLE_TASSERT(6721000);
    }
}

bool elementIsInfinite(BSONElement elt) {
    constexpr auto inf = std::numeric_limits<double>::infinity();
    if (elt.type() != BSONType::NumberDouble) {
        return false;
    }
    auto num = elt.Double();
    return num == inf || num == -inf;
}

std::unique_ptr<MatchExpression> makeEncryptedBetweenFromInterval(
    const ResolvedEncryptionInfo& metadata, StringData path, Interval interval) {

    auto kMinDouble = BSON("" << -std::numeric_limits<double>::infinity());
    auto kMaxDouble = BSON("" << std::numeric_limits<double>::infinity());

    if (interval.start.type() == BSONType::Date && interval.start.Date() == Date_t::min()) {
        interval.start = kMinDouble.firstElement();
    }
    if (interval.end.type() == BSONType::Date && interval.end.Date() == Date_t::max()) {
        interval.end = kMaxDouble.firstElement();
    }

    uassert(6747901,
            str::stream()
                << "Lower bound of range predicate must be within the bounds of encrypted index.",
            elementIsInfinite(interval.start) ||
                literalWithinRangeBounds(metadata, interval.start));

    uassert(6747902,
            str::stream()
                << "Upper bound of range predicate must be within the bounds of encrypted index.",
            elementIsInfinite(interval.end) || literalWithinRangeBounds(metadata, interval.end));

    auto ki = metadata.keyId.uuids()[0];
    // TODO: SERVER-67421 support multiple queries for a field.
    auto indexConfig = metadata.fle2SupportedQueries.get()[0];
    return buildEncryptedBetweenWithPlaceholder(path,
                                                ki,
                                                indexConfig,
                                                {interval.start, interval.startInclusive},
                                                {interval.end, interval.endInclusive});
}

/**
 * This helper takes inspiration from the shard routing code in mongos, which also creates a
 * "pseudo" index entry for the purposes of bounds detection.
 */
IndexEntry makeEntryForRange(const StringData& fieldpath) {
    return IndexEntry(BSON(fieldpath << 1),
                      IndexType::INDEX_ENCRYPTED_RANGE,
                      IndexDescriptor::kLatestIndexVersion,
                      // FLE does not support arrays, so encrypted indexes are never multikey.
                      false,
                      // Empty multikey paths, since the shard key index cannot be multikey.
                      MultikeyPaths{},
                      // Empty multikey path set, since the shard key index cannot be multikey.
                      {},
                      false /* sparse */,
                      false /* unique */,
                      IndexEntry::Identifier{fieldpath.toString()},
                      nullptr /* filterExpr */,
                      BSONObj(),
                      nullptr, /* collator */
                      nullptr /* projExec */);
}

bool isRangeComparison(MatchType t) {
    switch (t) {
        case MatchType::LT:
        case MatchType::LTE:
        case MatchType::GT:
        case MatchType::GTE:
            return true;
        default:
            return false;
    }
}
}  // namespace

void FLEMatchExpression::processRangesInAndClause(const EncryptionSchemaTreeNode& schemaTree,
                                                  AndMatchExpression* expr) {
    invariant(expr);
    std::map<std::string, std::unique_ptr<OrderedIntervalList>> ranges;
    auto* children = expr->getChildVector();
    auto it = children->begin();
    while (it != children->end()) {
        auto* child = it->get();
        if (!isRangeComparison(child->matchType())) {
            // All query operators besides range comparisons should be replaced normally.
            if (auto rangeReplaced = replaceEncryptedRangeElements(schemaTree, child)) {
                it->swap(rangeReplaced);
            }
            ++it;
            continue;
        }
        auto path = child->path().toString();
        auto metadata = schemaTree.getEncryptionMetadataForPath(FieldRef(path));
        if (!metadata) {
            // Range queries over non-encrypted fields can pass through unmodified and uninspected.
            ++it;
            continue;
        }
        uassert(6720400,
                str::stream() << "Invalid match expression operator on encrypted field '" << path
                              << "' without an encrypted range index: " << child->toString(),
                metadata->algorithmIs(Fle2AlgorithmInt::kRange));

        IndexBoundsBuilder::BoundsTightness tightnessOut;
        auto idx = makeEntryForRange(path);
        if (ranges.find(path) == ranges.end()) {
            auto oil = std::make_unique<OrderedIntervalList>();
            IndexBoundsBuilder::translate(
                child, BSONElement(), idx, oil.get(), &tightnessOut, nullptr);
            ranges.emplace(path, std::move(oil));
        } else {
            IndexBoundsBuilder::translateAndIntersect(
                child, BSONElement(), idx, ranges[path].get(), &tightnessOut, nullptr);
        }
        // Numeric types that are supported by always produce exact index bounds.
        invariant(tightnessOut == IndexBoundsBuilder::EXACT);
        // Now that the child node is contributing to an interval that will be included in a
        // $between, it should not be in the $and list.
        it = children->erase(it);
    }

    // Add a $between operator to the $and for every interval detected in the query.
    for (const auto& [path, oil] : ranges) {
        auto metadata = schemaTree.getEncryptionMetadataForPath(FieldRef(path));
        invariant(metadata);
        invariant(metadata->algorithmIs(Fle2AlgorithmInt::kRange));

        for (const auto& interval : oil->intervals) {
            _didMark = aggregate_expression_intender::Intention::Marked;
            children->emplace_back(
                makeEncryptedBetweenFromInterval(metadata.value(), path, interval));
        }
    }
}


std::unique_ptr<MatchExpression> FLEMatchExpression::replaceEncryptedRangeElements(
    const EncryptionSchemaTreeNode& schemaTree, MatchExpression* root) {
    invariant(root);
    switch (root->matchType()) {
        case MatchExpression::LTE:
        case MatchExpression::LT:
        case MatchExpression::GT:
        case MatchExpression::GTE: {
            auto compExpr = static_cast<ComparisonMatchExpression*>(root);
            auto metadata = schemaTree.getEncryptionMetadataForPath(FieldRef(compExpr->path()));

            uassert(6721001,
                    str::stream() << "Invalid match expression operator on encrypted field '"
                                  << root->path()
                                  << "' without an encrypted range index: " << root->toString(),
                    !metadata || metadata->algorithmIs(Fle2AlgorithmInt::kRange));
            if (!metadata) {
                // Don't recurse into objects on the RHS. Disallowing comparisons to objects will be
                // handled in the equality pass.
                return nullptr;
            }


            _didMark = aggregate_expression_intender::Intention::Marked;
            return makeOneSidedEncryptedBetween(metadata.value(), compExpr);
        }
        case MatchExpression::EQ: {
            auto eqExpr = static_cast<EqualityMatchExpression*>(root);
            auto metadata = schemaTree.getEncryptionMetadataForPath(FieldRef(eqExpr->path()));
            // Check to see if we have a range index. If we have one, assume there isn't an equality
            // index and rewrite to a $between.
            if (!metadata || !metadata->algorithmIs(Fle2AlgorithmInt::kRange)) {
                return nullptr;
            }
            _didMark = aggregate_expression_intender::Intention::Marked;
            return makeEncryptedBetweenFromInterval(
                metadata.value(),
                eqExpr->path(),
                // This is an equality, so only need to match one point.
                IndexBoundsBuilder::makePointInterval(BSON("" << eqExpr->getData())));
        }
        case MatchExpression::AND: {
            auto andExpr = static_cast<AndMatchExpression*>(root);
            auto originalNumChildren = andExpr->numChildren();
            processRangesInAndClause(schemaTree, andExpr);
            if (andExpr->numChildren() == 1 && originalNumChildren > 1) {
                // If the number of children has been reduced to 1, return the child as the new
                // node.
                return andExpr->getChild(0)->shallowClone();
            }
            return nullptr;
        }
        case MatchExpression::OR:
        case MatchExpression::NOT:
        case MatchExpression::NOR: {
            // Recursively descend each child of this expression.
            for (size_t index = 0; index < root->numChildren(); ++index) {
                if (auto newChild =
                        replaceEncryptedRangeElements(schemaTree, root->getChild(index))) {
                    root->resetChild(index, newChild.release());
                }
            }
            return nullptr;
        }
        case MatchExpression::EXPRESSION: {
            // TODO: SERVER-68031 Replace encrypted range predicates within $expr.
            return nullptr;
        }
        case MatchExpression::BETWEEN: {
            uasserted(6721005,
                      "$between should not be used in queries that go through implicit "
                      "encryption.");
        }
        case MatchExpression::ELEM_MATCH_OBJECT:
        case MatchExpression::ELEM_MATCH_VALUE:
        case MatchExpression::SIZE:
        case MatchExpression::REGEX:
        case MatchExpression::MOD:
        case MatchExpression::EXISTS:
        case MatchExpression::MATCH_IN:
        case MatchExpression::BITS_ALL_SET:
        case MatchExpression::BITS_ALL_CLEAR:
        case MatchExpression::BITS_ANY_SET:
        case MatchExpression::BITS_ANY_CLEAR:
        case MatchExpression::TYPE_OPERATOR:
        case MatchExpression::GEO:
        case MatchExpression::WHERE:
        case MatchExpression::ALWAYS_FALSE:
        case MatchExpression::ALWAYS_TRUE:
        case MatchExpression::GEO_NEAR:
        case MatchExpression::TEXT:
        case MatchExpression::INTERNAL_2D_POINT_IN_ANNULUS:
        case MatchExpression::INTERNAL_BUCKET_GEO_WITHIN:
        case MatchExpression::INTERNAL_EXPR_EQ:
        case MatchExpression::INTERNAL_EXPR_GT:
        case MatchExpression::INTERNAL_EXPR_GTE:
        case MatchExpression::INTERNAL_EXPR_LT:
        case MatchExpression::INTERNAL_EXPR_LTE:
        case MatchExpression::INTERNAL_SCHEMA_ALLOWED_PROPERTIES:
        case MatchExpression::INTERNAL_SCHEMA_ALL_ELEM_MATCH_FROM_INDEX:
        case MatchExpression::INTERNAL_SCHEMA_BIN_DATA_ENCRYPTED_TYPE:
        case MatchExpression::INTERNAL_SCHEMA_BIN_DATA_FLE2_ENCRYPTED_TYPE:
        case MatchExpression::INTERNAL_SCHEMA_BIN_DATA_SUBTYPE:
        case MatchExpression::INTERNAL_SCHEMA_COND:
        case MatchExpression::INTERNAL_SCHEMA_EQ:
        case MatchExpression::INTERNAL_SCHEMA_FMOD:
        case MatchExpression::INTERNAL_SCHEMA_MATCH_ARRAY_INDEX:
        case MatchExpression::INTERNAL_SCHEMA_MAX_ITEMS:
        case MatchExpression::INTERNAL_SCHEMA_MAX_LENGTH:
        case MatchExpression::INTERNAL_SCHEMA_MAX_PROPERTIES:
        case MatchExpression::INTERNAL_SCHEMA_MIN_ITEMS:
        case MatchExpression::INTERNAL_SCHEMA_MIN_LENGTH:
        case MatchExpression::INTERNAL_SCHEMA_MIN_PROPERTIES:
        case MatchExpression::INTERNAL_SCHEMA_OBJECT_MATCH:
        case MatchExpression::INTERNAL_SCHEMA_ROOT_DOC_EQ:
        case MatchExpression::INTERNAL_SCHEMA_TYPE:
        case MatchExpression::INTERNAL_SCHEMA_UNIQUE_ITEMS:
        case MatchExpression::INTERNAL_SCHEMA_XOR:
            return nullptr;
    }
    return nullptr;
}
}  // namespace mongo
