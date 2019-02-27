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

#include "encryption_schema_tree.h"

#include "mongo/bson/bsontypes.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/matcher/schema/json_schema_parser.h"
#include "mongo/util/string_map.h"

namespace mongo {

namespace {

// Declared early to permit mutual recursion.
std::unique_ptr<EncryptionSchemaTreeNode> _parse(BSONObj schema, bool encryptAllowed);

enum class SchemaTypeRestriction {
    kNone,    // No type restriction.
    kObject,  // Restricted on type "object" only.
    kOther,   // Type is specified but not one of the above.
};

/**
 * Returns the type restriction for the current schema based on the 'type' and 'bsonType' elements.
 * If not restricted, returns kNone.
 */
SchemaTypeRestriction getTypeRestriction(StringMap<BSONElement>& keywordMap) {
    auto getRestriction = [](BSONElement elem, const StringMap<BSONType>& aliasMap) {
        auto typeSet = uassertStatusOK(JSONSchemaParser::parseTypeSet(elem, aliasMap));

        // Check if the type element restricts the schema to an object. Note that 'type' can be an
        // array of string aliases including 'object'.
        return (typeSet.hasType(BSONType::Object) && typeSet.isSingleType())
            ? SchemaTypeRestriction::kObject
            : SchemaTypeRestriction::kOther;
    };

    if (auto typeElem = keywordMap[JSONSchemaParser::kSchemaTypeKeyword]) {
        return getRestriction(typeElem, MatcherTypeSet::kJsonSchemaTypeAliasMap);
    } else if (auto bsonTypeElem = keywordMap[JSONSchemaParser::kSchemaBsonTypeKeyword]) {
        return getRestriction(bsonTypeElem, kTypeAliasMap);
    } else {
        return SchemaTypeRestriction::kNone;
    }
}

/**
 * Parses the options under the 'encrypt' keyword, passed by the caller in 'encryptElt'. Returns a
 * pointer to the created encrypted node.
 *
 * As 'schema', the caller should supply the schema or subschema in which the 'encrypt' keyword is
 * specified in order to validate that 'encrypt' has no illegal sibling keywords.
 *
 * Throws if the caller supplies 'false' for 'encryptAllowed'.
 *
 * Note that this method does not perform full validation of each field (e.g. valid JSON Pointer
 * keyId) as it assumes this has already been done by the normal JSON Schema parser.
 */
std::unique_ptr<EncryptionSchemaEncryptedNode> parseEncrypt(BSONElement encryptElt,
                                                            BSONObj schema,
                                                            bool encryptAllowed) {
    uassert(51077,
            str::stream() << "Invalid schema containing the '"
                          << JSONSchemaParser::kSchemaEncryptKeyword
                          << "' keyword.",
            encryptAllowed);

    uassert(51078,
            str::stream() << "Invalid schema containing the '"
                          << JSONSchemaParser::kSchemaEncryptKeyword
                          << "' keyword, sibling keywords are not allowed as such restrictions "
                             "cannot work on an encrypted field.",
            schema.nFields() == 1U);

    const IDLParserErrorContext encryptCtxt("encrypt");

    // It's safe to skip the type check on 'encrypt' since the JSONSchemaParser should have already
    // validated it.
    EncryptionInfo encryptInfo = EncryptionInfo::parse(encryptCtxt, encryptElt.embeddedObject());

    // Manually build an EncryptionMetadata to attach to the schema tree.
    EncryptionMetadata encryptMetadata;
    encryptMetadata.setAlgorithm(encryptInfo.getAlgorithm());
    encryptMetadata.setInitializationVector(encryptInfo.getInitializationVector());
    encryptMetadata.setKeyId(encryptInfo.getKeyId());

    return std::make_unique<EncryptionSchemaEncryptedNode>(std::move(encryptMetadata));
}

/**
 * Throws an exception if an illegal 'encrypt' keyword is found inside an 'items' or
 * 'additionalItems' subschema.
 */
void validateArrayKeywords(StringMap<BSONElement>& keywordMap) {
    // Recurse each schema in items and verify that 'encrypt' is not specified.
    if (auto itemsElem = keywordMap[JSONSchemaParser::kSchemaItemsKeyword]) {
        if (itemsElem.type() == BSONType::Array) {
            for (auto&& subschema : itemsElem.embeddedObject()) {
                // Parse each nested schema, disallowing 'encrypt'. We can safely ignore the return
                // value since this method will throw before adding any encryption nodes.
                _parse(subschema.embeddedObject(), false);
            }
        } else if (itemsElem.type() == BSONType::Object) {
            // Parse the nested schema, disallowing 'encrypt'. We can safely ignore the return
            // value since this method will throw before adding any encryption nodes.
            _parse(itemsElem.embeddedObject(), false);
        }
    }

    // Verify that 'encrypt' is not specified in 'additionalItems'.
    if (auto additionalItemsElem = keywordMap[JSONSchemaParser::kSchemaAdditionalItemsKeyword]) {
        // Although the value of 'additionalItems' can be a boolean, we only need to do further
        // validation if it contains a nested schema. It is safe to ignore the return value since
        // this method will throw if the nested schema is invalid.
        if (additionalItemsElem.type() == BSONType::Object) {
            _parse(additionalItemsElem.embeddedObject(), false);
        }
    }
}

/**
 * Returns the encryption schema tree specified by the object keywords 'properties' and
 * 'additionalProperties'. The BSON elements associated with these object keywords are obtained from
 * 'keywordMap'. The caller must have already verified that 'encrypt' is not present in
 * 'keywordMap'.
 *
 * If 'encryptAllowed' is false, throws an exception upon encountering the 'encrypt' keyword in any
 * subschema.
 */
std::unique_ptr<EncryptionSchemaTreeNode> parseObjectKeywords(StringMap<BSONElement>& keywordMap,
                                                              bool encryptAllowed) {
    auto node = std::make_unique<EncryptionSchemaNotEncryptedNode>();

    // Check if the type of the current schema specifies type:"object". We only permit the 'encrypt'
    // keyword inside nested schemas if the current schema requires an object.
    SchemaTypeRestriction restriction = getTypeRestriction(keywordMap);

    bool encryptAllowedForSubschema =
        (restriction == SchemaTypeRestriction::kObject) ? encryptAllowed : false;

    // Recurse each nested schema in 'properties' and append the resulting nodes to the encryption
    // schema tree.
    if (auto propertiesElem = keywordMap[JSONSchemaParser::kSchemaPropertiesKeyword]) {
        for (auto&& property : propertiesElem.embeddedObject()) {
            node->addChild(std::string(property.fieldName()),
                           _parse(property.embeddedObject(), encryptAllowedForSubschema));
        }
    }

    // Handle the 'additionalProperties' keyword.
    if (auto additionalPropertiesElem =
            keywordMap[JSONSchemaParser::kSchemaAdditionalPropertiesKeyword]) {
        // We can ignore 'additionalProperties' when it is a boolean. It doesn't matter whether
        // additional properties are always allowed or always disallowed with respect to encryption;
        // we only need to add nodes to the encryption schema tree when 'additionalProperties'
        // contains a subschema.
        if (additionalPropertiesElem.type() == BSONType::Object) {
            node->addAdditionalPropertiesChild(
                _parse(additionalPropertiesElem.embeddedObject(), encryptAllowedForSubschema));
        }
    }

    return node;
}

/**
 * Parses the given schema and returns the root of the resulting encryption tree. If
 * 'encryptAllowed' is set to false, then this method will throw an assertion if any nested schema
 * contains the 'encrypt' keyword.
 *
 * The caller is expected to validate 'schema' before calling this function.
 */
std::unique_ptr<EncryptionSchemaTreeNode> _parse(BSONObj schema, bool encryptAllowed) {
    // Map of JSON Schema keywords which are relevant for encryption. To put a different way, the
    // resulting tree of encryption nodes is only affected by this list of keywords.
    StringMap<BSONElement> keywordMap{
        {std::string(JSONSchemaParser::kSchemaAdditionalItemsKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaAdditionalPropertiesKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaBsonTypeKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaEncryptKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaItemsKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaPropertiesKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaTypeKeyword), {}},
    };

    // Populate the keyword map for the list of relevant keywords for encryption. Can safely ignore
    // unknown keywords as full validation of the schema should've been handled already.
    for (auto&& elt : schema) {
        auto it = keywordMap.find(elt.fieldNameStringData());
        if (it == keywordMap.end())
            continue;

        keywordMap[elt.fieldNameStringData()] = elt;
    }

    validateArrayKeywords(keywordMap);

    if (auto encryptElem = keywordMap[JSONSchemaParser::kSchemaEncryptKeyword]) {
        return parseEncrypt(encryptElem, schema, encryptAllowed);
    }

    return parseObjectKeywords(keywordMap, encryptAllowed);
}

}  // namespace

std::unique_ptr<EncryptionSchemaTreeNode> EncryptionSchemaTreeNode::parse(BSONObj schema) {
    // Verify that the schema is valid by running through the normal JSONSchema parser, ignoring the
    // resulting match expression.
    uassertStatusOK(JSONSchemaParser::parse(schema));

    // The schema is at least syntatically valid, now build and return an encryption schema tree.
    return _parse(schema, true);
}

}  // namespace mongo
