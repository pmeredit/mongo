/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "query_analysis.h"

#include <stack>

#include "encryption_schema_tree.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/rpc/op_msg.h"

namespace mongo {

namespace {

static constexpr auto kJsonSchema = "jsonSchema"_sd;
static constexpr auto kHasEncryptionPlaceholders = "hasEncryptionPlaceholders"_sd;
static constexpr auto kResult = "result"_sd;

/**
 * Struct to hold information about placeholder results returned to client.
 */
struct PlaceHolderResult {
    bool hasEncryptionPlaceholders{false};

    BSONObj result;
};


BSONObj extractJSONSchema(BSONObj obj, BSONObjBuilder* stripped) {
    BSONObj ret;
    for (auto& e : obj) {
        if (e.fieldNameStringData() == kJsonSchema) {
            uassert(51090, "jsonSchema is expected to be a object", e.type() == Object);

            ret = e.Obj();
        } else {
            stripped->append(e);
        }
    }

    uassert(51073, "jsonSchema is a required command field", !ret.isEmpty());

    stripped->doneFast();

    return ret;
}

// TODO - Query implements this correctly
PlaceHolderResult addPlaceHoldersForFind(const BSONObj& cmdObj, const BSONObj& schema) {
    return PlaceHolderResult();
}

PlaceHolderResult addPlaceHoldersForAggregate(const BSONObj& cmdObj, const BSONObj& schema) {
    return PlaceHolderResult();
}

PlaceHolderResult addPlaceHoldersForCount(const BSONObj& cmdObj, const BSONObj& schema) {
    return PlaceHolderResult();
}

PlaceHolderResult addPlaceHoldersForDistinct(const BSONObj& cmdObj, const BSONObj& schema) {
    return PlaceHolderResult();
}

PlaceHolderResult addPlaceHoldersForFindAndModify(const BSONObj& cmdObj, const BSONObj& schema) {
    return PlaceHolderResult();
}


PlaceHolderResult addPlaceHoldersForInsert(const OpMsgRequest& request, const BSONObj& schema) {
    return PlaceHolderResult();
}

PlaceHolderResult addPlaceHoldersForUpdate(const OpMsgRequest& request, const BSONObj& schema) {
    return PlaceHolderResult();
}

PlaceHolderResult addPlaceHoldersForDelete(const OpMsgRequest& request, const BSONObj& schema) {
    return PlaceHolderResult();
}

void serializePlaceholderResult(const PlaceHolderResult& placeholder, BSONObjBuilder* builder) {
    builder->append(kHasEncryptionPlaceholders, placeholder.hasEncryptionPlaceholders);

    builder->append(kResult, placeholder.result);
}

OpMsgRequest makeHybrid(const OpMsgRequest& request, BSONObj body) {
    OpMsgRequest newRequest;
    newRequest.body = body;
    newRequest.sequences = request.sequences;
    return newRequest;
}

using WriteOpProcessFunction = PlaceHolderResult(const OpMsgRequest& request,
                                                 const BSONObj& jsonSchema);

void processWriteOpCommand(const OpMsgRequest& request,
                           BSONObjBuilder* builder,
                           WriteOpProcessFunction func) {
    BSONObjBuilder stripped;
    auto schema = extractJSONSchema(request.body, &stripped);

    auto newRequest = makeHybrid(request, stripped.obj());

    PlaceHolderResult placeholder = func(newRequest, schema);

    serializePlaceholderResult(placeholder, builder);
}


using QueryProcessFunction = PlaceHolderResult(const BSONObj& cmdObj, const BSONObj& jsonSchema);

void processQueryCommand(const BSONObj& cmdObj,
                         BSONObjBuilder* builder,
                         QueryProcessFunction func) {
    BSONObjBuilder stripped;
    auto schema = extractJSONSchema(cmdObj, &stripped);

    PlaceHolderResult placeholder = func(stripped.obj(), schema);

    serializePlaceholderResult(placeholder, builder);
}

}  // namespace

bool isEncryptionNeeded(const BSONObj& jsonSchema) {
    auto schemaNode = EncryptionSchemaTreeNode::parse(jsonSchema);
    std::stack<EncryptionSchemaTreeNode*> nodeStack;
    nodeStack.push(schemaNode.get());
    while (!nodeStack.empty()) {
        EncryptionSchemaTreeNode* curNode = nodeStack.top();
        nodeStack.pop();
        if (curNode->getEncryptionMetadata()) {
            return true;
        }
        for (auto&& it : *curNode) {
            nodeStack.push(it.second.get());
        }
    }
    return false;
}


void processFindCommand(const BSONObj& cmdObj, BSONObjBuilder* builder) {
    processQueryCommand(cmdObj, builder, addPlaceHoldersForFind);
}

void processAggregateCommand(const BSONObj& cmdObj, BSONObjBuilder* builder) {
    processQueryCommand(cmdObj, builder, addPlaceHoldersForAggregate);
}

void processDistinctCommand(const BSONObj& cmdObj, BSONObjBuilder* builder) {
    processQueryCommand(cmdObj, builder, addPlaceHoldersForDistinct);
}

void processCountCommand(const BSONObj& cmdObj, BSONObjBuilder* builder) {
    processQueryCommand(cmdObj, builder, addPlaceHoldersForCount);
}

void processFindAndModifyCommand(const BSONObj& cmdObj, BSONObjBuilder* builder) {
    processQueryCommand(cmdObj, builder, addPlaceHoldersForFindAndModify);
}

void processInsertCommand(const OpMsgRequest& request, BSONObjBuilder* builder) {
    processWriteOpCommand(request, builder, addPlaceHoldersForInsert);
}

void processUpdateCommand(const OpMsgRequest& request, BSONObjBuilder* builder) {
    processWriteOpCommand(request, builder, addPlaceHoldersForUpdate);
}

void processDeleteCommand(const OpMsgRequest& request, BSONObjBuilder* builder) {
    processWriteOpCommand(request, builder, addPlaceHoldersForDelete);
}

}  // namespace mongo
