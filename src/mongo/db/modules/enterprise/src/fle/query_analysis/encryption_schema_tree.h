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

#pragma once

#include <pcrecpp.h>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/field_ref.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

/**
 * A class that represents a node in an encryption schema tree.
 *
 * The children of this node are accessed via a string map, where the key used in the map represents
 * the next path component.
 *
 * Example schema with encrypted fields "user.ssn" and "account":
 *
 * {$jsonSchema: {
 *      type: "object",
 *      properties: {
 *          user: {
 *              type: "object",
 *              properties: {
 *                  ssn: {encrypt:{}},
 *                  address: {type: "string"}
 *              }
 *          },
 *          account: {encrypt: {}},
 *      }
 * }}
 *
 * Results in the following encryption schema tree:
 *
 *                   NotEncryptedNode
 *                       /    \
 *                 user /      \ account
 *                     /        \
 *        NotEncryptedNode    EncryptedNode
 *               /     \
 *          ssn /       \ address
 *             /         \
 *     EncryptedNode  NotEncryptedNode
 */
class EncryptionSchemaTreeNode {
public:
    /**
     * Converts a JSON schema, represented as BSON, into an encryption schema tree. Returns a
     * pointer to the root of the tree or throws an exception if either the schema is invalid or is
     * valid but illegal from an encryption analysis perspective.
     */
    static std::unique_ptr<EncryptionSchemaTreeNode> parse(BSONObj schema);

    virtual ~EncryptionSchemaTreeNode() = default;

    void addChild(std::string path, std::unique_ptr<EncryptionSchemaTreeNode> node) {
        _propertiesChildren[std::move(path)] = std::move(node);
    }

    /**
     * Adds 'node' as a special "wildcard" child which is used for all field names that don't have
     * explicit child nodes. For instance, consider the schema
     *
     * {
     *   type: "object",
     *   properties: {a: {type: "number"}, b: {type: "string"}},
     *   required: ["a", "b"],
     *   additionalProperties: {encrypt: {}}
     * }
     *
     * This schema matches objects where "a" is number, "b" is a string, and all other properties
     * are encrypted. This requires a special child in the encryption tree which has no particular
     * field name associated with it:
     *
     *                   NotEncryptedNode
     *                  /    |           \
     *               a /     | b          \ *
     *                /      |             \
     *  NotEncryptedNode  NotEncryptedNode  EncryptedNode
     *
     * The "*" in the diagram above indicates wildcard behavior: this child applies for all field
     * names other than "a" and "b".
     */
    void addAdditionalPropertiesChild(std::unique_ptr<EncryptionSchemaTreeNode> node) {
        _additionalPropertiesChild = std::move(node);
    }

    /**
     * Adds 'node' as a special child associated with a regular expression rather than a fixed field
     * name. For instance, consider the schema
     *
     * {
     *   type: "object",
     *   properties: {a: {type: "number"}, b: {type: "string"}},
     *   patternProperties: {"^c": {encrypt: {}}}
     * }
     *
     * This schema matches objects where "a" is a number (if it exists), "b" is a string (if it
     * exists), and any property names which begin with "c" are encrypted. The 'patternProperties'
     * keyword results in a node in the encryption tree which is associated with the regex /^c/. The
     * encryption schema tree would look like this:
     *
     *                   NotEncryptedNode
     *                  /    |           \
     *               a /     | b          \ /^c/
     *                /      |             \
     *  NotEncryptedNode  NotEncryptedNode  EncryptedNode
     */
    void addPatternPropertiesChild(StringData regex,
                                   std::unique_ptr<EncryptionSchemaTreeNode> node) {
        _patternPropertiesChildren.emplace_back(regex, std::move(node));
    }

    /**
     * If the given path maps to an encryption node in the tree then returns the associated
     * EncryptionMetadata, otherwise returns boost::none. Any numerical path components will
     * *always* be treated as field names, not array indexes.
     */
    boost::optional<EncryptionMetadata> getEncryptionMetadataForPath(const FieldRef& path) const {
        return _getEncryptionMetadataForPath(path, 0);
    }

    /**
     * Returns true if this tree contains at least one EncryptionSchemaEncryptedNode.
     */
    virtual bool containsEncryptedNode() const;

    /**
     * Override this method to return the node's EncryptionMetadata, or boost::none if it holds
     * none.
     */
    virtual boost::optional<EncryptionMetadata> getEncryptionMetadata() const = 0;

private:
    struct PatternPropertiesChild {
        PatternPropertiesChild(StringData regexStringData,
                               std::unique_ptr<EncryptionSchemaTreeNode> child)
            : regex(std::make_unique<pcrecpp::RE>(regexStringData.toString())),
              child(std::move(child)) {
            const auto& errorStr = regex->error();
            uassert(51141,
                    str::stream() << "Invalid regular expression in 'patternProperties': "
                                  << regexStringData
                                  << " PCRE error string: "
                                  << errorStr,
                    errorStr.empty());
        }

        std::unique_ptr<pcrecpp::RE> regex;
        std::unique_ptr<EncryptionSchemaTreeNode> child;
    };

    /**
     * Given the property name 'name', returns a list of child nodes for the subschemas that are
     * relevant. This follows the rules associated with the JSON Schema 'properties',
     * 'patternProperties', and 'additionalProperties' keywords. If there is a child added to the
     * tree via addChild() with the edge name exactly matching 'name', then that child will be
     * included in the output list. In addition, children added via addPatternPropertiesChild()
     * whose regex matches 'name' will be included in the output list.
     *
     * If no regular addChild() nodes or 'patternProperties' child nodes are found, but a node has
     * been added via addAdditionalPropertiesChild(), then returns this 'additionalProperties'
     * child.
     *
     * If no child with 'name' exists, no 'patternProperties' child whose regex matches 'name'
     * exists, and there is no 'additionalProperties' child, then returns an empty vector.
     */
    std::vector<EncryptionSchemaTreeNode*> getChildrenForPathComponent(StringData name) const;

    /**
     * This method is responsible for recursively descending the encryption tree until the end of
     * the path is reached or there's no edge to take. The 'index' parameter is used to indicate
     * which part of 'path' we're currently at, and is expected to increment as we descend the tree.
     *
     * Throws an AssertionException if 'path' contains a prefix to an encrypted field.
     *
     * Throws if multiple relevant subschemas return conflicting encryption metadata. This can
     * happen for 'patternProperties', since we may need to descend the subtrees for multiple
     * matching patterns.
     */
    boost::optional<EncryptionMetadata> _getEncryptionMetadataForPath(const FieldRef& path,
                                                                      size_t index = 0) const;

    StringMap<std::unique_ptr<EncryptionSchemaTreeNode>> _propertiesChildren;

    // Holds any children which are associated with a regex rather than a specific field name.
    std::vector<PatternPropertiesChild> _patternPropertiesChildren;

    // If non-null, this special child is used when no applicable child is found by name in
    // '_propertiesChildren' or by regex in '_patternPropertiesChildren'. Used to implement
    // encryption analysis for the 'additionalProperties' keyword.
    std::unique_ptr<EncryptionSchemaTreeNode> _additionalPropertiesChild;
};

/**
 * Represents a path that is not encrypted. May be either an internal node or a leaf node.
 */
class EncryptionSchemaNotEncryptedNode final : public EncryptionSchemaTreeNode {
public:
    boost::optional<EncryptionMetadata> getEncryptionMetadata() const final {
        return boost::none;
    }
};

/**
 * Node which represents an encrypted field per the corresponding JSON Schema. A path is considered
 * encrypted only if it's final component lands on this node.
 */
class EncryptionSchemaEncryptedNode final : public EncryptionSchemaTreeNode {
public:
    EncryptionSchemaEncryptedNode(EncryptionMetadata metadata) : _metadata(std::move(metadata)) {
        uassert(51099,
                "Encrypt object combined with encryptMetadata needs to specify an algorithm",
                _metadata.getAlgorithm());
        uassert(51096,
                "Deterministic algorithm must be accompanied with an initialization vector in "
                "encrypt object combined with encryptMetadata",
                _metadata.getAlgorithm() == FleAlgorithmEnum::kRandom ||
                    _metadata.getInitializationVector());
        uassert(51097,
                "Encrypt object combined with encryptMetadata needs to specify a keyId",
                _metadata.getKeyId());
    }

    boost::optional<EncryptionMetadata> getEncryptionMetadata() const final {
        return _metadata;
    }

    bool containsEncryptedNode() const final {
        return true;
    }

private:
    const EncryptionMetadata _metadata;
};

}  // namespace mongo
