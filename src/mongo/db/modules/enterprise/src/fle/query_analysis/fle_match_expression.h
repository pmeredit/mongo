/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "aggregate_expression_intender.h"
#include "encryption_schema_tree.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/matcher/expression_expr.h"
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
        return _didMark == aggregate_expression_intender::Intention::Marked;
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
     *
     * Throws if 'collator' is non-null (i.e. the collation is non-simple) and 'elem' is of a type
     * that would require a collation-aware comparison comparison.
     */
    BSONElement allocateEncryptedElement(const BSONElement& elem,
                                         const ResolvedEncryptionInfo& metadata,
                                         const CollatorInterface* collator);

    /**
     * Wraps 'encryptedObj' as the only element within a single-field parent object, and holds a
     * refcount to this newly created parent object. Returns the resulting BSONElement.
     */
    BSONElement allocateEncryptedObject(BSONObj encryptedObj) {
        _encryptedElements.push_back(BSON("" << encryptedObj));
        _didMark = aggregate_expression_intender::Intention::Marked;
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

    // Keeps track of whether or not this expression has any placeholders marked for encryption.
    aggregate_expression_intender::Intention _didMark =
        aggregate_expression_intender::Intention::NotMarked;
};

}  // namespace mongo
