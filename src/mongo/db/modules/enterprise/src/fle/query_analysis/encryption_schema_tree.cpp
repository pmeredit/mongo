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

#include <list>

namespace mongo {

namespace {

// Declared early to permit mutual recursion.
class EncryptMetadataChainMemento;
std::unique_ptr<EncryptionSchemaTreeNode> _parse(BSONObj schema,
                                                 bool encryptAllowed,
                                                 EncryptMetadataChainMemento metadataChain);

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
 * Memento class facilitating managing of an internal list of EncryptMetadata objects across
 * recursive invocations during parsing of a JSON schema.
 *
 * An object is created at each recursive invocation of the _parse() method but the same
 * internal list is passed across. If a new EncryptMetadata gets added at a given recursive
 * call, this fact will be recorded in the memento. Once the call is concluded this new
 * element will be popped automatically.
 */
class EncryptMetadataChainMemento {
public:
    EncryptMetadataChainMemento(std::list<EncryptionMetadata>& chain)
        : _chain(chain), _wasMetadataPushed(false) {}

    EncryptMetadataChainMemento(const EncryptMetadataChainMemento& src)
        : _chain(src._chain), _wasMetadataPushed(false) {}

    ~EncryptMetadataChainMemento() {
        if (_wasMetadataPushed)
            _chain.pop_back();
    }

    void push(EncryptionMetadata metadata) {
        uassert(51098,
                str::stream()
                    << "At most one EncryptMetadata object can be specified at a given level.",
                !_wasMetadataPushed);

        _chain.push_back(std::move(metadata));
        _wasMetadataPushed = true;
    }

    /**
     * Computes metadata for a Encrypt node taking into account metadata objects inherited on the
     * way from the root.
     */
    EncryptionMetadata combineWithChain(const EncryptionInfo& encryptInfo) const {
        EncryptionMetadata metadata;

        // Combine metadata chain from the root to current element.
        for (const auto& newMetadata : _chain) {
            if (newMetadata.getAlgorithm())
                metadata.setAlgorithm(newMetadata.getAlgorithm().value());
            if (newMetadata.getInitializationVector())
                metadata.setInitializationVector(newMetadata.getInitializationVector().value());
            if (newMetadata.getKeyId())
                metadata.setKeyId(newMetadata.getKeyId().value());
        }

        // Override non-empty fields of the combined metadata with the fields from
        // Encrypt element, as they take precedence.
        if (encryptInfo.getAlgorithm())
            metadata.setAlgorithm(encryptInfo.getAlgorithm());
        if (encryptInfo.getInitializationVector())
            metadata.setInitializationVector(encryptInfo.getInitializationVector());
        if (encryptInfo.getKeyId())
            metadata.setKeyId(encryptInfo.getKeyId());
        return metadata;
    }

private:
    // Chain of EncryptionMetadata for which we hold this memento.
    std::list<EncryptionMetadata>& _chain;

    // Indicates if metadata was added to the list.
    bool _wasMetadataPushed;
};

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
std::unique_ptr<EncryptionSchemaEncryptedNode> parseEncrypt(
    BSONElement encryptElt,
    BSONObj schema,
    bool encryptAllowed,
    const EncryptMetadataChainMemento& metadataChain) {
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

    EncryptionInfo encryptInfo = EncryptionInfo::parse(encryptCtxt, encryptElt.embeddedObject());
    auto metadata = metadataChain.combineWithChain(encryptInfo);
    return std::make_unique<EncryptionSchemaEncryptedNode>(metadata);
}

/**
 * Throws an exception if an illegal 'encrypt' keyword is found inside of a subschema for an array
 * keyword ('items' or 'additionalItems') or for a logical keyword ('allOf', 'anyOf', 'oneOf', or
 * 'not').
 *
 * We currently make no attempt to simplify or analyze schemas written using the logical keywords.
 */
void validateArrayAndLogicalSubschemas(StringMap<BSONElement>& keywordMap,
                                       const EncryptMetadataChainMemento& metadataChain) {
    // Recurse each schema in items and verify that 'encrypt' is not specified.
    if (auto itemsElem = keywordMap[JSONSchemaParser::kSchemaItemsKeyword]) {
        if (itemsElem.type() == BSONType::Array) {
            for (auto&& subschema : itemsElem.embeddedObject()) {
                // Parse each nested schema, disallowing 'encrypt'. We can safely ignore the return
                // value since this method will throw before adding any encryption nodes.
                _parse(subschema.embeddedObject(), false, metadataChain);
            }
        } else if (itemsElem.type() == BSONType::Object) {
            // Parse the nested schema, disallowing 'encrypt'. We can safely ignore the return
            // value since this method will throw before adding any encryption nodes.
            _parse(itemsElem.embeddedObject(), false, metadataChain);
        }
    }

    // Verify that 'encrypt' is not specified in 'additionalItems'.
    if (auto additionalItemsElem = keywordMap[JSONSchemaParser::kSchemaAdditionalItemsKeyword]) {
        // Although the value of 'additionalItems' can be a boolean, we only need to do further
        // validation if it contains a nested schema. It is safe to ignore the return value since
        // this method will throw if the nested schema is invalid.
        if (additionalItemsElem.type() == BSONType::Object) {
            _parse(additionalItemsElem.embeddedObject(), false, metadataChain);
        }
    }

    // Several of the logical keywords take an array of subschemas.
    for (auto&& arrayKeyword : {JSONSchemaParser::kSchemaAllOfKeyword,
                                JSONSchemaParser::kSchemaAnyOfKeyword,
                                JSONSchemaParser::kSchemaOneOfKeyword}) {
        if (auto arrayKeywordElem = keywordMap[arrayKeyword]) {
            for (auto&& subschema : arrayKeywordElem.embeddedObject()) {
                _parse(subschema.embeddedObject(), false, metadataChain);
            }
        }
    }

    // Ensure that the subschema for the 'not' logical keyword has no 'encrypt' specifiers.
    if (auto notElem = keywordMap[JSONSchemaParser::kSchemaNotKeyword]) {
        _parse(notElem.embeddedObject(), false, metadataChain);
    }
}

/**
 * Returns the encryption schema tree specified by the object keywords 'properties',
 * 'patternProperties', and 'additionalProperties'. The BSON elements associated with these object
 * keywords are obtained from 'keywordMap'. The caller must have already verified that 'encrypt' is
 * not present in 'keywordMap'.
 *
 * If 'encryptAllowed' is false, throws an exception upon encountering the 'encrypt' keyword in any
 * subschema.
 */
std::unique_ptr<EncryptionSchemaTreeNode> parseObjectKeywords(
    StringMap<BSONElement>& keywordMap,
    bool encryptAllowed,
    const EncryptMetadataChainMemento& metadataChain) {
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
            auto fieldName = property.fieldNameStringData();
            // Ban dotted field names in schema. They can incorrectly be treated as paths by the
            // server's JSON Schema implementation. SERVER-31493 would fix this.
            bool encryptAllowedForField =
                encryptAllowedForSubschema && fieldName.find('.') == std::string::npos;
            node->addChild(
                fieldName.rawData(),
                _parse(property.embeddedObject(), encryptAllowedForField, metadataChain));
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
            node->addAdditionalPropertiesChild(_parse(additionalPropertiesElem.embeddedObject(),
                                                      encryptAllowedForSubschema,
                                                      metadataChain));
        }
    }

    // Handle the 'patternProperties' keyword.
    if (auto patternPropertiesElem =
            keywordMap[JSONSchemaParser::kSchemaPatternPropertiesKeyword]) {
        // 'patternProperties' must be an object, which should have been validated upstream.
        // Similarly, each property inside the 'patternProperties' must itself be an object.
        for (auto&& pattern : patternPropertiesElem.embeddedObject()) {
            node->addPatternPropertiesChild(
                pattern.fieldNameStringData(),
                _parse(pattern.embeddedObject(), encryptAllowedForSubschema, metadataChain));
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
std::unique_ptr<EncryptionSchemaTreeNode> _parse(BSONObj schema,
                                                 bool encryptAllowed,
                                                 EncryptMetadataChainMemento metadataChain) {
    // Map of JSON Schema keywords which are relevant for encryption. To put a different way, the
    // resulting tree of encryption nodes is only affected by this list of keywords.
    StringMap<BSONElement> keywordMap{
        {std::string(JSONSchemaParser::kSchemaAdditionalItemsKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaAdditionalPropertiesKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaAllOfKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaAnyOfKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaBsonTypeKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaEncryptKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaEncryptMetadataKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaItemsKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaNotKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaOneOfKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaPatternPropertiesKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaPropertiesKeyword), {}},
        {std::string(JSONSchemaParser::kSchemaTypeKeyword), {}},
    };

    // Populate the keyword map for the list of relevant keywords for encryption.
    for (auto&& elt : schema) {
        auto it = keywordMap.find(elt.fieldNameStringData());
        uassert(31068,
                str::stream() << "JSON schema keyword '" << elt.fieldNameStringData()
                              << "' is not supported for client-side encryption",
                it != keywordMap.end());

        keywordMap[elt.fieldNameStringData()] = elt;
    }

    validateArrayAndLogicalSubschemas(keywordMap, metadataChain);

    if (auto encryptElem = keywordMap[JSONSchemaParser::kSchemaEncryptKeyword]) {
        return parseEncrypt(encryptElem, schema, encryptAllowed, metadataChain);
    }

    if (auto encryptMetadataElt = keywordMap[JSONSchemaParser::kSchemaEncryptMetadataKeyword]) {
        uassert(31077,
                str::stream() << "Invalid schema containing the '"
                              << JSONSchemaParser::kSchemaEncryptMetadataKeyword
                              << "' keyword.",
                encryptAllowed);
        IDLParserErrorContext ctxt("encryptMetadata");
        const auto& metadata = EncryptionMetadata::parse(ctxt, encryptMetadataElt.embeddedObject());

        metadataChain.push(metadata);
    }

    return parseObjectKeywords(keywordMap, encryptAllowed, metadataChain);
}

}  // namespace

bool EncryptionSchemaTreeNode::_containsEncryptedNodeBelowPrefix(const FieldRef& prefix,
                                                                 size_t level) const {
    invariant(!getEncryptionMetadata());
    if (level >= prefix.numParts()) {
        return containsEncryptedNode();
    }
    auto matchingChildren = getChildrenForPathComponent(prefix.getPart(level));
    for (auto const& child : matchingChildren) {
        if (child->_containsEncryptedNodeBelowPrefix(prefix, level + 1)) {
            return true;
        }
    }
    return false;
}

std::unique_ptr<EncryptionSchemaTreeNode> EncryptionSchemaTreeNode::parse(BSONObj schema) {
    // Verify that the schema is valid by running through the normal JSONSchema parser, ignoring the
    // resulting match expression.
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(nullptr, nullptr));
    uassertStatusOK(JSONSchemaParser::parse(expCtx, schema));

    // The schema is at least syntatically valid, now build and return an encryption schema tree.
    // Inheritance of EncryptMetadata is implemented by passing around a chain of metadata
    // predecessors.
    std::list<EncryptionMetadata> metadataChain;
    return _parse(schema, true, metadataChain);
}

std::vector<EncryptionSchemaTreeNode*> EncryptionSchemaTreeNode::getChildrenForPathComponent(
    StringData name) const {
    std::vector<EncryptionSchemaTreeNode*> matchingChildren;
    auto it = _propertiesChildren.find(name.toString());
    if (it != _propertiesChildren.end()) {
        matchingChildren.push_back(it->second.get());
    }

    for (auto && [ regex, child ] : _patternPropertiesChildren) {
        if (regex.PartialMatch(
                pcrecpp::StringPiece{name.rawData(), static_cast<int>(name.size())})) {
            matchingChildren.push_back(child.get());
        }
    }

    // We only consider the child for 'additionalProperties' if there are no relevant children from
    // 'properties' or 'patternProperties'.
    if (_additionalPropertiesChild && matchingChildren.empty()) {
        matchingChildren.push_back(_additionalPropertiesChild.get());
    }
    return matchingChildren;
}

boost::optional<EncryptionMetadata> EncryptionSchemaTreeNode::_getEncryptionMetadataForPath(
    const FieldRef& path, size_t index) const {
    // If we've ended on this node, then return whether its an encrypted node.
    if (index >= path.numParts()) {
        return getEncryptionMetadata();
    }

    auto children = getChildrenForPathComponent(path[index]);
    if (children.empty()) {
        // If there's no path to take from the current node, then we're in one of two cases:
        //  * The current node is an EncryptNode. This means that the query path has an
        //    encrypted field as its prefix. No such query can ever succeed when sent to the
        //    server, so we throw in this case.
        //  * The path does not exist in the schema tree. In this case, we return boost::none to
        //    indicate that the path is not encrypted.
        uassert(51102,
                str::stream() << "Invalid operation on path '" << path.dottedField()
                              << "' which contains an encrypted path prefix.",
                !getEncryptionMetadata());

        return boost::none;
    }

    // There is at least one relevant child. Recursively get the encryption metadata from this child
    // schema.
    auto metadata = children[0]->_getEncryptionMetadataForPath(path, index + 1);

    // Verify that all additional child schemas report the same encryption metadata as the first.
    auto it = children.begin();
    ++it;
    for (; it != children.end(); ++it) {
        auto nextChild = *it;
        auto additionalMetadata = nextChild->_getEncryptionMetadataForPath(path, index + 1);
        uassert(51142,
                str::stream() << "Found conflicting encryption metadata for path: '"
                              << path.dottedField()
                              << "'",
                additionalMetadata == metadata);
    }

    return metadata;
};

bool EncryptionSchemaTreeNode::containsEncryptedNode() const {
    for (auto && [ path, child ] : _propertiesChildren) {
        if (child->containsEncryptedNode()) {
            return true;
        }
    }

    for (auto&& patternPropertiesChild : _patternPropertiesChildren) {
        if (patternPropertiesChild.child->containsEncryptedNode()) {
            return true;
        }
    }

    if (_additionalPropertiesChild) {
        return _additionalPropertiesChild->containsEncryptedNode();
    }

    return false;
}

std::unique_ptr<EncryptionSchemaTreeNode>
clonable_traits<EncryptionSchemaTreeNode>::clone_factory_type::operator()(
    const EncryptionSchemaTreeNode& input) const {
    return input.clone();
}

}  // namespace mongo
