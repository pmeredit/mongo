/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "query_analysis.h"

#include <stack>

#include "encryption_schema_tree.h"
#include "mongo/bson/bsonobj.h"

namespace mongo {

bool isEncryptionNeeded(const BSONObj& jsonSchema) {
    auto schemaNode = EncryptionSchemaTreeNode::parse(jsonSchema);
    std::stack<EncryptionSchemaTreeNode*> nodeStack;
    nodeStack.push(schemaNode.get());
    while (!nodeStack.empty()) {
        EncryptionSchemaTreeNode* curNode = nodeStack.top();
        nodeStack.pop();
        if (curNode->isEncryptedLeafNode()) {
            return true;
        }
        for (auto&& it : *curNode) {
            nodeStack.push(it.second.get());
        }
    }
    return false;
}

}  // namespace mongo
