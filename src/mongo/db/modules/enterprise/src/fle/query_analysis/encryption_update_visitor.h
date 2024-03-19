/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <vector>

#include "encryption_schema_tree.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/update/object_replace_executor.h"
#include "mongo/db/update/pipeline_executor.h"
#include "mongo/db/update/rename_node.h"
#include "mongo/db/update/set_node.h"
#include "mongo/db/update/update_internal_node.h"
#include "mongo/db/update/update_node_visitor.h"
#include "mongo/db/update/update_object_node.h"
#include "query_analysis.h"

namespace mongo {

/**
 * A class to traverse an update tree and replace to-be-encrypted fields with encryption
 * placeholders.
 */
class EncryptionUpdateVisitor final : public UpdateNodeVisitor {
public:
    EncryptionUpdateVisitor(const EncryptionSchemaTreeNode& tree) : _schemaTree(tree) {}

    void visit(AddToSetNode* host) override {
        throwOnEncryptedPath("$addToSet");
    }

    void visit(ArithmeticNode* host) override {
        throwOnEncryptedPath("$inc and $mul");
    }

    void visit(BitNode* host) override {
        throwOnEncryptedPath("$bit");
    }

    void visit(CompareNode* host) override {
        throwOnEncryptedPath("$max and $min");
    }

    /**
     * This node is part of a $rename operation. EncryptionUpdateVisitor does all necessary work
     * for $rename in the RenameNode visitor, so no work is needed here.
     */
    void visit(ConflictPlaceholderNode* host) override {}

    void visit(CurrentDateNode* host) override {
        throwOnEncryptedPath("$currentDate");
    }

    void visit(PopNode* host) override {
        throwOnEncryptedPath("$pop");
    }

    void visit(PullAllNode* host) override {
        throwOnEncryptedPath("$pullAll");
    }

    void visit(PullNode* host) override {
        throwOnEncryptedPath("$pull");
    }
    void visit(PushNode* host) override {
        throwOnEncryptedPath("$push");
    }

    void visit(RenameNode* host) override {
        FieldRef sourcePath{host->getValue().fieldNameStringData()};
        auto sourceMetadata = _schemaTree.getEncryptionMetadataForPath(sourcePath);
        auto destinationMetadata = _schemaTree.getEncryptionMetadataForPath(_currentPath);

        uassert(6329901,
                "$rename between encrypted fields is not permitted with Queryable Encryption",
                (!sourceMetadata || !sourceMetadata->isFle2Encrypted()) &&
                    (!destinationMetadata || !destinationMetadata->isFle2Encrypted()));
        uassert(51160,
                "$rename between two encrypted fields must have the same metadata or both be "
                "unencrypted",
                sourceMetadata == destinationMetadata);
        uassert(51161,
                "$rename is not allowed on an object containing encrypted fields",
                sourceMetadata ||
                    (!_schemaTree.mayContainEncryptedNodeBelowPrefix(sourcePath) &&
                     !_schemaTree.mayContainEncryptedNodeBelowPrefix(
                         FieldRef{host->getValue().String()})));
    }

    /**
     * This node is part of a $rename operation. EncryptionUpdateVisitor does all necessary work
     * for $rename in the RenameNode visitor, so no work is needed here.
     */
    void visit(SetElementNode* host) override {}

    /**
     * $set is not allowed to remove an encrypted field. This asserts that no part of 'setVal' is
     * the prefix of an encrypted field and does not set a value for that encrypted field.
     */
    void verifySetSchemaOK(BSONElement setVal, FieldRef prefix) {
        // If the prefix is encrypted, or has no encrypted nodes below it in the tree, this path is
        // not removing any encrypted fields.
        if (_schemaTree.getEncryptionMetadataForPath(prefix) ||
            !_schemaTree.mayContainEncryptedNodeBelowPrefix(prefix)) {
            return;
        }
        uassert(51159,
                "Cannot $set to a path " + prefix.dottedField() +
                    " that is an encrypted prefix to a non-object type",
                setVal.type() == BSONType::Object);

        for (auto&& element : setVal.embeddedObject()) {
            FieldRef::FieldRefTempAppend tempAppend(prefix, element.fieldNameStringData());
            verifySetSchemaOK(element, prefix);
        }
    }

    void visit(SetNode* host) override {
        if (auto metadata = _schemaTree.getEncryptionMetadataForPath(_currentPath)) {
            // We need not pass through a collator here, even if the update command has a collation
            // specified, because the $set does not make collation-aware comparisons. It is legal
            // to use $set to create an encrypted string field, even if the update operation has a
            // non-simple collation.
            const CollatorInterface* collator = nullptr;
            auto placeholder =
                buildEncryptPlaceholder(host->val,
                                        metadata.get(),
                                        query_analysis::EncryptionPlaceholderContext::kWrite,
                                        collator);
            _backingBSONs.push_back(placeholder);
            // The object returned by 'buildEncryptPlaceholder' only has one element.
            host->val = placeholder.firstElement();
        } else {
            verifySetSchemaOK(host->val, _currentPath);
            if (host->val.type() == BSONType::Object) {
                // If the right hand side of the $set is an object, recursively check if it contains
                // any encrypted fields.
                //
                // We need not pass through a collator here, even if the update command has a
                // collation specified, because the $set does not make collation-aware comparisons.
                // It is legal to use $set to create an encrypted string field, even if the update
                // operation has a non-simple collation.
                const CollatorInterface* collator = nullptr;
                auto placeholder =
                    replaceEncryptedFields(host->val.embeddedObject(),
                                           &_schemaTree,
                                           query_analysis::EncryptionPlaceholderContext::kWrite,
                                           _currentPath,
                                           boost::none,
                                           collator);
                if (placeholder.hasEncryptionPlaceholders) {
                    auto finalBSON = BSON(host->val.fieldNameStringData() << placeholder.result);
                    host->val = finalBSON.firstElement();
                    _backingBSONs.push_back(finalBSON);
                }
            }
        }
    }

    void visit(UnsetNode*) override {
        // No work to do for $unset on mongocryptd.
    }

    void visit(UpdateArrayNode* host) override {
        throwOnEncryptedPath("Array update operations");
    }

    void visit(UpdateObjectNode* host) override {
        uassert(51149,
                "Cannot encrypt fields below '$' positional update operator",
                !host->getChild("$") ||
                    !(_schemaTree.getEncryptionMetadataForPath(_currentPath) ||
                      _schemaTree.mayContainEncryptedNodeBelowPrefix(_currentPath)));

        for (const auto& [pathSuffix, child] : host->getChildren()) {
            FieldRef::FieldRefTempAppend tempAppend(_currentPath, pathSuffix);
            child->acceptVisitor(this);
        }
    }

    bool hasPlaceholder() const {
        return !_backingBSONs.empty();
    }

private:
    void throwOnEncryptedPath(StringData operatorName) {
        uassert(51150,
                str::stream() << operatorName << " not allowed on encrypted values",
                !_schemaTree.getEncryptionMetadataForPath(_currentPath));
    }

    FieldRef _currentPath;
    const EncryptionSchemaTreeNode& _schemaTree;
    std::vector<BSONObj> _backingBSONs;
};
}  // namespace mongo
