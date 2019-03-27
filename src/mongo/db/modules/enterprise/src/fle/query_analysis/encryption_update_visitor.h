/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <vector>

#include "encryption_schema_tree.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/update/object_replace_node.h"
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

    /**
     * This node is part of a $rename operation. EncryptionUpdateVisitor does all necessary work
     * for $rename in the RenameNode visitor, so no work is needed here.
     */
    void visit(ConflictPlaceholderNode* host) {}

    void visit(CurrentDateNode* host) {
        throwOnEncryptedPath("$currentDate");
    }

    void visit(ObjectReplaceNode* host) {
        auto placeholder = replaceEncryptedFields(host->val, &_schemaTree, FieldRef{}, boost::none);
        if (placeholder.hasEncryptionPlaceholders) {
            host->val = placeholder.result;
            _hasPlaceholder = true;
        }
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
        FieldRef sourcePath{host->getValue().fieldNameStringData()};
        auto sourceMetadata = _schemaTree.getEncryptionMetadataForPath(sourcePath);
        auto destinationMetadata = _schemaTree.getEncryptionMetadataForPath(_currentPath);
        uassert(51160,
                "$rename between two encrypted fields must have the same metadata",
                sourceMetadata == destinationMetadata);
        uassert(51161,
                "$rename is not allowed on an object containing encrypted fields",
                sourceMetadata || (!_schemaTree.containsEncryptedNodeBelowPrefix(sourcePath) &&
                                   !_schemaTree.containsEncryptedNodeBelowPrefix(
                                       FieldRef{host->getValue().String()})));
    }

    /**
     * This node is part of a $rename operation. EncryptionUpdateVisitor does all necessary work
     * for $rename in the RenameNode visitor, so no work is needed here.
     */
    void visit(SetElementNode* host) {}

    /**
     * $set is not allowed to remove an encrypted field. This asserts that no part of 'setVal' is
     * the prefix of an encrypted field and does not set a value for that encrypted field.
     */
    void verifySetSchemaOK(BSONElement setVal, FieldRef prefix) {
        // If the prefix is encrypted, or has no encrypted nodes below it in the tree, this path is
        // not removing any encrypted fields.
        if (_schemaTree.getEncryptionMetadataForPath(prefix) ||
            !_schemaTree.containsEncryptedNodeBelowPrefix(prefix)) {
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

    void visit(SetNode* host) {
        if (auto metadata = _schemaTree.getEncryptionMetadataForPath(_currentPath)) {
            // TODO: SERVER-40217 Error if modifying an array.
            auto placeholder = buildEncryptPlaceholder(host->val, metadata.get());
            _backingBSONs.push_back(placeholder);
            _hasPlaceholder = true;
            // The object returned by 'buildEncryptPlaceholder' only has one element.
            host->val = placeholder.firstElement();
        } else {
            verifySetSchemaOK(host->val, _currentPath);
            if (host->val.type() == BSONType::Object) {
                // If the right hand side of the $set is an object, recursively check if it contains
                // any encrypted fields.
                auto placeholder = replaceEncryptedFields(
                    host->val.embeddedObject(), &_schemaTree, _currentPath, boost::none);
                if (placeholder.hasEncryptionPlaceholders) {
                    auto finalBSON = BSON(host->val.fieldNameStringData() << placeholder.result);
                    host->val = finalBSON.firstElement();
                    _backingBSONs.push_back(finalBSON);
                    _hasPlaceholder = true;
                }
            }
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
        return _hasPlaceholder;
    }

private:
    void throwOnEncryptedPath(StringData operatorName) {
        uassert(51150,
                str::stream() << operatorName << " not allowed on encrypted values",
                !_schemaTree.getEncryptionMetadataForPath(_currentPath));
    }

    bool _hasPlaceholder = false;
    FieldRef _currentPath;
    const EncryptionSchemaTreeNode& _schemaTree;
    std::vector<BSONObj> _backingBSONs;
};
}  // namespace mongo
