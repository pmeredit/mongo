/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <vector>

#include "encryption_schema_tree.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/update/object_replace_node.h"
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

    void visit(AddToSetNode* host) {
        throwOnEncryptedPath("$addToSet");
    }

    void visit(ArithmeticNode* host) {
        throwOnEncryptedPath("$inc and $mul");
    }

    void visit(BitNode* host) {
        throwOnEncryptedPath("$bit");
    }

    void visit(CompareNode* host) {
        throwOnEncryptedPath("$max and $min");
    }

    void visit(ConflictPlaceholderNode* host) {
        // TODO: SERVER-39254 handle $rename.
        uasserted(ErrorCodes::CommandNotSupported, "$rename not yet supported on mongocryptd");
    }

    void visit(CurrentDateNode* host) {
        throwOnEncryptedPath("$currentDate");
    }

    void visit(ObjectReplaceNode* host) {
        // TODO: SERVER-39277
        uasserted(ErrorCodes::CommandNotSupported,
                  "Object replacement not yet supported on mongocryptd");
    }

    void visit(PopNode* host) {
        throwOnEncryptedPath("$pop");
    }

    void visit(PullAllNode* host) {
        throwOnEncryptedPath("$pullAll");
    }

    void visit(PullNode* host) {
        throwOnEncryptedPath("$pull");
    }
    void visit(PushNode* host) {
        throwOnEncryptedPath("$push");
    }

    void visit(RenameNode* host) {
        // TODO: SERVER-39254
        uasserted(ErrorCodes::CommandNotSupported, "$rename not yet supported on mongocryptd");
    }

    void visit(SetElementNode* host) {
        uasserted(ErrorCodes::CommandNotSupported, "$rename not yet supported on mongocryptd");
    }

    void visit(SetNode* host) {
        if (auto metadata = _schemaTree.getEncryptionMetadataForPath(_currentPath)) {
            // TODO: SERVER-40217 Error if modifying an array.
            auto placeholder = buildEncryptPlaceholder(host->val, metadata.get());
            _backingBSONs.push_back(placeholder);
            // The object returned by 'buildEncryptPlaceholder' only has one element.
            host->val = placeholder.firstElement();
        }
    }

    void visit(UnsetNode* host) {
        // TODO: SERVER-40234 handle $unset.
        uasserted(ErrorCodes::CommandNotSupported, "$unset not yet supported on mongocryptd");
    }

    void visit(UpdateArrayNode* host) {
        throwOnEncryptedPath("Array update operations");
    }

    void visit(UpdateObjectNode* host) {
        // TODO: SERVER-40208 Investigate whether there are scenarios where we can handle this case.
        uassert(51149,
                "Cannot encrypt update with '$' positional update operator",
                !host->getChild("$"));
        for (const auto & [ pathSuffix, child ] : host->getChildren()) {
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
