/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "query_analysis.h"

#include <stack>

#include "encryption_schema_tree.h"
#include "fle_match_expression.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/ops/write_ops.h"
#include "mongo/db/query/query_request.h"
#include "mongo/rpc/op_msg.h"

namespace mongo {

namespace {

static constexpr auto kJsonSchema = "jsonSchema"_sd;
static constexpr auto kHasEncryptionPlaceholders = "hasEncryptionPlaceholders"_sd;
static constexpr auto kResult = "result"_sd;

/**
 * Extracts and returns the jsonSchema field in the command 'obj'. Populates 'stripped' with the
 * same fields as 'obj', except without the jsonSchema.
 *
 * Throws an AssertionException if the schema is missing or not of the correct type.
 */
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

/**
 * Recursively descends through the doc curDoc. For each key in the doc, checks the schema and
 * replaces it with an encryption placeholder if neccessary.
 * Does not descend into arrays.
 */
BSONObj replaceEncryptedFieldsRecursive(const EncryptionSchemaTreeNode* schema,
                                        BSONObj curDoc,
                                        FieldRef* leadingPath,
                                        bool* encryptedFieldFound) {
    BSONObjBuilder builder;
    for (auto&& element : curDoc) {
        auto fieldName = element.fieldNameStringData();
        leadingPath->appendPart(fieldName);
        if (auto metadata = schema->getEncryptionMetadataForPath(*leadingPath)) {
            // TODO SERVER-39958: Error on Arrays.
            *encryptedFieldFound = true;
            BSONObj placeholder = buildEncryptPlaceholder(element, metadata.get());
            builder.append(placeholder[fieldName]);
        } else if (element.type() != BSONType::Object) {
            // Encrypt markings below arrays are not supported.
            builder.append(element);
        } else {
            builder.append(fieldName,
                           replaceEncryptedFieldsRecursive(
                               schema, element.embeddedObject(), leadingPath, encryptedFieldFound));
        }
        leadingPath->removeLastPart();
    }
    return builder.obj();
}

PlaceHolderResult addPlaceHoldersForFind(const BSONObj& cmdObj,
                                         std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    // Parse to a QueryRequest to ensure the command syntax is valid. We can use a temporary
    // database name however the collection name will be used when serializing back to BSON.
    auto qr = uassertStatusOK(
        QueryRequest::makeFromFindCommand(NamespaceString(cmdObj["$db"].checkAndGetStringData(),
                                                          cmdObj["find"].checkAndGetStringData()),
                                          cmdObj,
                                          false));

    // Build a parsed MatchExpression from the filter, allowing all special features.
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(nullptr, nullptr));
    auto matchExpr = uassertStatusOK(
        MatchExpressionParser::parse(qr->getFilter(),
                                     expCtx,
                                     ExtensionsCallbackNoop(),
                                     MatchExpressionParser::kAllowAllSpecialFeatures));

    // Build a FLEMatchExpression, which will replace encrypted values with their appropriate
    // intent-to-encrypt markings.
    FLEMatchExpression fleMatchExpr(std::move(matchExpr), schemaTree.get());

    // Replace the previous filter object with the new MatchExpression after marking it for
    // encryption.
    BSONObjBuilder bob;
    fleMatchExpr.getMatchExpression()->serialize(&bob);
    qr->setFilter(bob.obj());

    return PlaceHolderResult{fleMatchExpr.containsEncryptedPlaceholders(), qr->asFindCommand()};
}

PlaceHolderResult addPlaceHoldersForAggregate(
    const BSONObj& cmdObj, std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    return PlaceHolderResult();
}

PlaceHolderResult addPlaceHoldersForCount(const BSONObj& cmdObj,
                                          std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    return PlaceHolderResult();
}

PlaceHolderResult addPlaceHoldersForDistinct(const BSONObj& cmdObj,
                                             std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    return PlaceHolderResult();
}

PlaceHolderResult addPlaceHoldersForFindAndModify(
    const BSONObj& cmdObj, std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    return PlaceHolderResult();
}

PlaceHolderResult addPlaceHoldersForInsert(const OpMsgRequest& request,
                                           std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    auto batch = InsertOp::parse(request);
    auto docs = batch.getDocuments();
    PlaceHolderResult retPlaceholder;
    std::vector<BSONObj> docVector;
    for (const BSONObj& doc : docs) {
        auto placeholderPair = replaceEncryptedFields(doc, schemaTree.get());
        retPlaceholder.hasEncryptionPlaceholders =
            retPlaceholder.hasEncryptionPlaceholders || placeholderPair.hasEncryptionPlaceholders;
        docVector.push_back(placeholderPair.result);
    }
    batch.setDocuments(docVector);
    retPlaceholder.result = batch.toBSON(request.body);
    return retPlaceholder;
}

PlaceHolderResult addPlaceHoldersForUpdate(const OpMsgRequest& request,
                                           std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    return PlaceHolderResult();
}

PlaceHolderResult addPlaceHoldersForDelete(const OpMsgRequest& request,
                                           std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
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

using WriteOpProcessFunction = PlaceHolderResult(
    const OpMsgRequest& request, std::unique_ptr<EncryptionSchemaTreeNode> schemaTree);

void processWriteOpCommand(const OpMsgRequest& request,
                           BSONObjBuilder* builder,
                           WriteOpProcessFunction func) {
    BSONObjBuilder stripped;
    auto schema = extractJSONSchema(request.body, &stripped);

    auto newRequest = makeHybrid(request, stripped.obj());

    // Parse the JSON Schema to an encryption schema tree.
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);

    PlaceHolderResult placeholder = func(newRequest, std::move(schemaTree));

    serializePlaceholderResult(placeholder, builder);
}

using QueryProcessFunction =
    PlaceHolderResult(const BSONObj& cmdObj, std::unique_ptr<EncryptionSchemaTreeNode> schemaTree);

void processQueryCommand(const BSONObj& cmdObj,
                         BSONObjBuilder* builder,
                         QueryProcessFunction func) {
    BSONObjBuilder stripped;
    auto schema = extractJSONSchema(cmdObj, &stripped);

    // Parse the JSON Schema to an encryption schema tree.
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);

    PlaceHolderResult placeholder = func(stripped.obj(), std::move(schemaTree));

    serializePlaceholderResult(placeholder, builder);
}

}  // namespace

PlaceHolderResult replaceEncryptedFields(BSONObj doc, const EncryptionSchemaTreeNode* schema) {
    PlaceHolderResult res;
    FieldRef leadingPath;
    res.result =
        replaceEncryptedFieldsRecursive(schema, doc, &leadingPath, &res.hasEncryptionPlaceholders);
    return res;
}

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

BSONObj buildEncryptPlaceholder(BSONElement elem, const EncryptionMetadata& metadata) {
    invariant(metadata.getAlgorithm());
    invariant(metadata.getKeyId());

    EncryptionPlaceholder marking(metadata.getAlgorithm().get(), EncryptSchemaAnyType(elem));

    if (marking.getAlgorithm() == FleAlgorithmEnum::kDeterministic) {
        invariant(metadata.getInitializationVector());
        marking.setInitializationVector(metadata.getInitializationVector().get());
    }

    auto keyId = metadata.getKeyId();
    uassert(51093,
            "A non-static (JSONPointer) keyId is not supported.",
            keyId.get().type() == EncryptSchemaKeyId::Type::kUUIDs);
    marking.setKeyId(keyId.get().uuids()[0]);

    // Serialize the placeholder to BSON.
    BSONObjBuilder bob;
    marking.serialize(&bob);
    auto markingObj = bob.done();

    // Encode the placeholder BSON as BinData (sub-type 6 for encryption). Prepend the sub-subtype
    // byte represent the intent-to-encrypt marking before the BSON payload.
    BufBuilder binDataBuffer;
    binDataBuffer.appendChar(0);
    binDataBuffer.appendBuf(markingObj.objdata(), markingObj.objsize());

    BSONObjBuilder binDataBob;
    binDataBob.appendBinData(
        elem.fieldNameStringData(), binDataBuffer.len(), BinDataType::Encrypt, binDataBuffer.buf());
    return binDataBob.obj();
}

}  // namespace mongo
