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

#include "encryption_schema_tree.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/matcher/expression_leaf.h"

namespace mongo {

/**
 * Represents a MatchExpression which has been mutated based on an encryption schema tree to contain
 * intent-to-encrypt markings.
 */
class FLEMatchExpression {
public:
    /**
     * Recursively descends the MatchExpression tree from 'root', replacing unencrypted values with
     * their placeholder according to the schema. This method throws an assertion if the
     * MatchExpression contains an invalid operator on an encrypted field.
     */
    FLEMatchExpression(std::unique_ptr<MatchExpression> expression,
                       const EncryptionSchemaTreeNode& schemaTree);

    MatchExpression* getMatchExpression() const {
        return _expression.get();
    }

    /**
     * Returns true if the underlying MatchExpression contains any EncryptionPlaceholders.
     */
    bool containsEncryptedPlaceholders() const {
        return _encryptedElements.size() > 0;
    }

private:
    /**
     * Marks RHS elements in 'root' as encrypted if a field is found to be encrypted per the
     * encryption schema tree.
     */
    void replaceEncryptedElements(const EncryptionSchemaTreeNode& schemaTree,
                                  MatchExpression* root);

    /**
     * Creates an object with a single BinData element representing the encryption placeholder for
     * 'elem', using the options in 'metadata'. Returns a BSONElement containing the resulting
     * BinData.
     */
    BSONElement allocateEncryptedElement(const BSONElement& elem,
                                         const EncryptionMetadata& metadata);

    /**
     * Wraps 'encryptedObj' as the only element within a single-field parent object, and holds a
     * refcount to this newly created parent object. Returns the resulting BSONElement.
     */
    BSONElement allocateEncryptedObject(BSONObj encryptedObj) {
        _encryptedElements.push_back(BSON("" << encryptedObj));
        return _encryptedElements.back().firstElement();
    }

    /**
     * Helper methods to replace encrypted elements in the corresponding match expression.
     */
    void replaceElementsInEqExpression(const EncryptionSchemaTreeNode& schemaTree,
                                       EqualityMatchExpression* expr);
    void replaceElementsInInExpression(const EncryptionSchemaTreeNode& schemaTree,
                                       InMatchExpression* expr);

    // Backing storage for any elements in the MatchExpression which have been marked for
    // encryption.
    std::vector<BSONObj> _encryptedElements;
    std::unique_ptr<MatchExpression> _expression;
};

}  // namespace mongo
