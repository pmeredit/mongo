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

#include "mongo/bson/bsonobj.h"
#include "mongo/db/field_ref.h"
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
 *                     ObjectNode
 *                       /    \
 *                 user /      \ account
 *                     /        \
 *              ObjectNode      EncryptedNode
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
        _children[std::move(path)] = std::move(node);
    }

    /**
     * Returns the child node for edge 'name', or returns nullptr if no child with 'name' exists.
     */
    EncryptionSchemaTreeNode* getChild(StringData name) const {
        auto it = _children.find(name.toString());
        return it != _children.end() ? it->second.get() : nullptr;
    }

    /**
     * Returns true if the given path maps to an encryption node in the tree, otherwise returns
     * false. Any numerical path components will *always* be treated as field names, not array
     * indexes.
     */
    bool isEncrypted(const FieldRef& path) const {
        return _isEncrypted(path, 0);
    }

    /**
     * Override this method to indicate whether this node holds encryption metadata.
     */
    virtual bool isEncryptedLeafNode() const = 0;

    StringMap<std::unique_ptr<EncryptionSchemaTreeNode>>::const_iterator begin() const {
        return _children.begin();
    }

    StringMap<std::unique_ptr<EncryptionSchemaTreeNode>>::const_iterator end() const {
        return _children.end();
    }

private:
    /**
     * This method is responsible for recursively descending the encryption tree until the end of
     * the path is reached or there's no edge to take. The 'index' parameter is used to indicate
     * which part of 'path' we're currently at, and is expected to increment as we descend the tree.
     */
    bool _isEncrypted(const FieldRef& path, size_t index = 0) const {
        // If we've ended on this node, then return whether its an encrypted node.
        if (index >= path.numParts()) {
            return isEncryptedLeafNode();
        }

        auto child = getChild(path[index]);
        if (!child) {
            // No path to take for the current path, return not encrypted.
            return false;
        }

        return child->_isEncrypted(path, index + 1);
    };

    StringMap<std::unique_ptr<EncryptionSchemaTreeNode>> _children;
};

/**
 * Node for a path to an object in the encryption schema tree.
 */
class EncryptionSchemaObjectNode final : public EncryptionSchemaTreeNode {
public:
    bool isEncryptedLeafNode() const final {
        return false;
    }
};

/**
 * Node which represents an encrypted field per the corresponding JSON Schema. A path is considered
 * encrypted only if it's final component lands on this node.
 */
class EncryptionSchemaEncryptedNode final : public EncryptionSchemaTreeNode {
public:
    bool isEncryptedLeafNode() const final {
        return true;
    }
};

/**
 * A placeholder class for a path which is not encrypted.
 */
class EncryptionSchemaNotEncryptedNode final : public EncryptionSchemaTreeNode {
public:
    bool isEncryptedLeafNode() const final {
        return false;
    }
};

}  // namespace mongo
