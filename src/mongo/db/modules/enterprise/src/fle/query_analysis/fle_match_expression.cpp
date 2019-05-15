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

#include "fle_match_expression.h"

#include "mongo/db/matcher/expression_leaf.h"
#include "mongo/db/matcher/schema/expression_internal_schema_eq.h"
#include "query_analysis.h"

namespace mongo {

using MatchType = MatchExpression::MatchType;
using EncryptionPlaceholderContext = cryptd_query_analysis::EncryptionPlaceholderContext;

FLEMatchExpression::FLEMatchExpression(std::unique_ptr<MatchExpression> expression,
                                       const EncryptionSchemaTreeNode& schemaTree)
    : _expression(std::move(expression)) {
    replaceEncryptedElements(schemaTree, _expression.get());
}

BSONElement FLEMatchExpression::allocateEncryptedElement(const BSONElement& elem,
                                                         const ResolvedEncryptionInfo& metadata,
                                                         const CollatorInterface* collator) {
    _encryptedElements.push_back(buildEncryptPlaceholder(
        elem, metadata, EncryptionPlaceholderContext::kComparison, collator));
    return _encryptedElements.back().firstElement();
}

void FLEMatchExpression::replaceElementsInEqExpression(const EncryptionSchemaTreeNode& schemaTree,
                                                       EqualityMatchExpression* eqExpr) {
    if (auto encryptMetadata = schemaTree.getEncryptionMetadataForPath(FieldRef(eqExpr->path()))) {
        // Queries involving comparisons to null cannot work with encryption, as the expected
        // semantics involve returning documents where the encrypted field is missing, null, or
        // undefined. Building an encryption placeholder with a null element will only return
        // documents with the literal null, not missing or undefined.
        uassert(51095,
                str::stream() << "Illegal equality to null predicate for encrypted field: '"
                              << eqExpr->path()
                              << "'",
                !eqExpr->getData().isNull());

        eqExpr->setData(allocateEncryptedElement(
            eqExpr->getData(), encryptMetadata.get(), eqExpr->getCollator()));
    } else {
        // The path to the $eq expression is not encrypted, however there may still be an encrypted
        // field within the RHS object.
        auto rhsElem = eqExpr->getData();
        if (rhsElem.type() == BSONType::Object) {
            auto[hasEncrypt, _, placeholder] =
                replaceEncryptedFields(rhsElem.embeddedObject(),
                                       &schemaTree,
                                       EncryptionPlaceholderContext::kComparison,
                                       FieldRef(eqExpr->path()),
                                       boost::none,
                                       eqExpr->getCollator());
            if (hasEncrypt) {
                eqExpr->setData(allocateEncryptedObject(placeholder));
            }
        } else if (rhsElem.type() == BSONType::Array) {
            uassert(
                31007,
                str::stream() << "$eq to array predicate is illegal for prefix of encrypted path: "
                              << eqExpr->toString(),
                !schemaTree.containsEncryptedNodeBelowPrefix(FieldRef{eqExpr->path()}));
        }
    }
}

void FLEMatchExpression::replaceElementsInInExpression(const EncryptionSchemaTreeNode& schemaTree,
                                                       InMatchExpression* inExpr) {
    std::vector<BSONElement> replacedElements;
    if (auto encryptMetadata = schemaTree.getEncryptionMetadataForPath(FieldRef(inExpr->path()))) {
        uassert(51015,
                str::stream() << "Illegal regex inside $in against an encrypted field: '"
                              << inExpr->path()
                              << "'",
                inExpr->getRegexes().empty());

        // Replace each element in the $in expression with its encryption placeholder.
        for (auto&& elem : inExpr->getEqualities()) {
            uassert(
                51120,
                str::stream() << "Illegal equality to null inside $in against an encrypted field: '"
                              << inExpr->path()
                              << "'",
                !elem.isNull());
            replacedElements.push_back(
                allocateEncryptedElement(elem, encryptMetadata.get(), inExpr->getCollator()));
        }
    } else {
        // The path to the $in expression is not encrypted, however there may still be an
        // encrypted field within any RHS objects of the $in array.
        bool hasPlaceholders = false;
        for (auto&& elem : inExpr->getEqualities()) {
            if (elem.type() == BSONType::Object) {
                auto[elemHasEncrypt, _, placeholder] =
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
                    replacedElements.push_back(allocateEncryptedObject(placeholder));
                } else {
                    replacedElements.push_back(elem);
                }

                hasPlaceholders = hasPlaceholders || elemHasEncrypt;
            } else if (elem.type() == BSONType::Array) {
                uassert(31008,
                        str::stream() << "Illegal $in element for prefix '" << inExpr->path()
                                      << "' of encrypted path: "
                                      << elem,
                        !schemaTree.containsEncryptedNodeBelowPrefix(FieldRef{inExpr->path()}));
                replacedElements.push_back(elem);
            } else {
                replacedElements.push_back(elem);
            }
        }
    }

    uassertStatusOK(inExpr->setEqualities(std::move(replacedElements)));
}

void FLEMatchExpression::replaceEncryptedElements(const EncryptionSchemaTreeNode& schemaTree,
                                                  MatchExpression* root) {
    invariant(root);

    switch (root->matchType()) {
        // Whitelist of expressions which are allowed on encrypted fields.
        case MatchType::EQ: {
            replaceElementsInEqExpression(schemaTree, static_cast<EqualityMatchExpression*>(root));
            break;
        }
        case MatchType::MATCH_IN: {
            replaceElementsInInExpression(schemaTree, static_cast<InMatchExpression*>(root));
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
        case MatchType::EXPRESSION:
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

        // Leaf expressions which are not allowed to operate on encrypted fields. Some of these
        // expressions may not contain sensitive data but the query itself does not make sense on an
        // encrypted field.
        case MatchType::BITS_ALL_SET:
        case MatchType::BITS_ALL_CLEAR:
        case MatchType::BITS_ANY_SET:
        case MatchType::BITS_ANY_CLEAR:
        case MatchType::ELEM_MATCH_OBJECT:
        case MatchType::ELEM_MATCH_VALUE:
        case MatchType::GEO:
        case MatchType::GEO_NEAR:
        case MatchType::INTERNAL_2D_POINT_IN_ANNULUS:
        case MatchType::INTERNAL_EXPR_EQ:
        case MatchType::INTERNAL_SCHEMA_ALL_ELEM_MATCH_FROM_INDEX:
        case MatchType::INTERNAL_SCHEMA_BIN_DATA_ENCRYPTED_TYPE:
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
                                  << root->path()
                                  << "': "
                                  << root->toString(),
                    !schemaTree.getEncryptionMetadataForPath(FieldRef(root->path())));
            break;

        case MatchType::GT:
        case MatchType::GTE:
        case MatchType::LTE:
        case MatchType::LT: {
            uassert(51118,
                    str::stream() << "Invalid match expression operator on encrypted field '"
                                  << root->path()
                                  << "': "
                                  << root->toString(),
                    !schemaTree.getEncryptionMetadataForPath(FieldRef(root->path())));
            // For comparison match expressions, also reject encrypted fields within RHS objects of
            // the expression.
            auto compExpr = static_cast<ComparisonMatchExpression*>(root);
            auto rhsElem = compExpr->getData();
            if (rhsElem.type() == BSONType::Object) {
                auto[hasEncrypt, _, placeholder] =
                    replaceEncryptedFields(rhsElem.embeddedObject(),
                                           &schemaTree,
                                           EncryptionPlaceholderContext::kComparison,
                                           FieldRef(compExpr->path()),
                                           boost::none,
                                           compExpr->getCollator());
                uassert(51119,
                        str::stream() << "Invalid match expression operator on encrypted field '"
                                      << root->toString()
                                      << "'",
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
        replaceEncryptedElements(schemaTree, root->getChild(index));
    }
}

}  // namespace mongo
