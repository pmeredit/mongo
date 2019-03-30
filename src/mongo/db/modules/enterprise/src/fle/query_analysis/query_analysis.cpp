/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "query_analysis.h"

#include <stack>

#include "encryption_schema_tree.h"
#include "encryption_update_visitor.h"
#include "fle_match_expression.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/commands.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/matcher/expression_type.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/ops/write_ops.h"
#include "mongo/db/ops/write_ops_gen.h"
#include "mongo/db/query/count_command_gen.h"
#include "mongo/db/query/distinct_command_gen.h"
#include "mongo/db/query/find_and_modify_request.h"
#include "mongo/db/query/query_request.h"
#include "mongo/db/update/update_driver.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/rpc/op_msg.h"

namespace mongo {

namespace {

static constexpr auto kJsonSchema = "jsonSchema"_sd;
static constexpr auto kHasEncryptionPlaceholders = "hasEncryptionPlaceholders"_sd;
static constexpr auto kSchemaRequiresEncryption = "schemaRequiresEncryption"_sd;
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
 * replaces it with an encryption placeholder if neccessary. If origDoc is given it is passed to
 * buildEncryptPlaceholders to resolve JSON Pointers. If it is not given, schemas with pointers
 * will error.
 * Does not descend into arrays.
 */
BSONObj replaceEncryptedFieldsRecursive(const EncryptionSchemaTreeNode* schema,
                                        BSONObj curDoc,
                                        const boost::optional<BSONObj>& origDoc,
                                        FieldRef* leadingPath,
                                        bool* encryptedFieldFound) {
    BSONObjBuilder builder;
    for (auto&& element : curDoc) {
        auto fieldName = element.fieldNameStringData();
        leadingPath->appendPart(fieldName);
        if (auto metadata = schema->getEncryptionMetadataForPath(*leadingPath)) {
            uassert(31005,
                    str::stream() << "encrypted path '" << leadingPath->dottedField()
                                  << "' cannot contain an array",
                    element.type() != BSONType::Array);
            *encryptedFieldFound = true;
            BSONObj placeholder =
                buildEncryptPlaceholder(element, metadata.get(), origDoc, *schema);
            builder.append(placeholder[fieldName]);
        } else if (element.type() == BSONType::Object) {
            builder.append(
                fieldName,
                replaceEncryptedFieldsRecursive(
                    schema, element.embeddedObject(), origDoc, leadingPath, encryptedFieldFound));
        } else if (element.type() == BSONType::Array) {
            // Encrypting beneath an array is not supported. If the user has an array along an
            // encrypted path, they have violated the type:"object" condition of the schema. For
            // example, if the user's schema indicates that "foo.bar" is encrypted, it is implied
            // that "foo" is an object. We should therefore return an error if the user attempts to
            // insert a document such as {foo: [{bar: 1}, {bar: 2}]}.
            //
            // Although mongocryptd doesn't enforce the provided JSON Schema in full, we make an
            // effort here to enforce the encryption-related aspects of the schema. Ensuring that
            // there are no arrays along an encrypted path falls within this mandate.
            uassert(31006,
                    str::stream() << "An array at path '" << leadingPath->dottedField()
                                  << "' would violate the schema",
                    !schema->containsEncryptedNodeBelowPrefix(*leadingPath));
            builder.append(element);
        } else {
            builder.append(element);
        }

        leadingPath->removeLastPart();
    }
    return builder.obj();
}

/**
 * Returns a new BSONObj that has all of the fields from 'response' that are also in 'original'.
 */
BSONObj removeExtraFields(const std::set<StringData>& original, const BSONObj& response) {
    BSONObjBuilder bob;
    for (auto&& elem : response) {
        if (original.find(elem.fieldNameStringData()) != original.end()) {
            bob.append(elem);
        }
    }
    return bob.obj();
}

/**
 * Parses the MatchExpression given by 'filter' and replaces encrypted fields with their appropriate
 * EncryptionPlaceholder according to the schema.
 *
 * Returns a PlaceHolderResult containing the new MatchExpression and a boolean to indicate whether
 * it contains an intent-to-encrypt marking. Throws an assertion if the MatchExpression is invalid
 * or contains an invalid operator over an encrypted field.
 */
PlaceHolderResult replaceEncryptedFieldsInFilter(const EncryptionSchemaTreeNode& schemaTree,
                                                 BSONObj filter) {
    // Build a parsed MatchExpression from the filter, allowing all special features.
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(nullptr, nullptr));
    auto matchExpr = uassertStatusOK(MatchExpressionParser::parse(
        filter, expCtx, ExtensionsCallbackNoop(), MatchExpressionParser::kAllowAllSpecialFeatures));

    // Build a FLEMatchExpression, which will replace encrypted values with their appropriate
    // intent-to-encrypt markings.
    FLEMatchExpression fleMatchExpr(std::move(matchExpr), schemaTree);

    // Replace the previous filter object with the new MatchExpression after marking it for
    // encryption.
    BSONObjBuilder bob;
    fleMatchExpr.getMatchExpression()->serialize(&bob);

    return {fleMatchExpr.containsEncryptedPlaceholders(),
            schemaTree.containsEncryptedNode(),
            bob.obj()};
}

/**
 * Parses the update object given by 'update' and replaces encrypted fields with their appropriate
 * EncryptionPlaceholder according to the schema.
 *
 * Returns a PlaceHolderResult containing the new update object and a boolean to indicate whether
 * it contains an intent-to-encrypt marking. Throws an assertion if the update object is invalid
 * or contains an invalid operator over an encrypted field.
 */
PlaceHolderResult replaceEncryptedFieldsInUpdate(const EncryptionSchemaTreeNode& schemaTree,
                                                 BSONObj update) {
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(nullptr, nullptr));

    UpdateDriver driver(expCtx);
    // Ignoring array filters as they are not relevant for encryption.
    driver.parse(update, {});
    auto updateVisitor = EncryptionUpdateVisitor(schemaTree);
    driver.visitRoot(&updateVisitor);
    return PlaceHolderResult{updateVisitor.hasPlaceholder(),
                             schemaTree.containsEncryptedNode(),
                             driver.serialize().getDocument().toBson()};
}

PlaceHolderResult addPlaceHoldersForFind(const std::string& dbName,
                                         const BSONObj& cmdObj,
                                         std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    // Parse to a QueryRequest to ensure the command syntax is valid. We can use a temporary
    // database name however the collection name will be used when serializing back to BSON.
    auto qr = uassertStatusOK(QueryRequest::makeFromFindCommand(
        CommandHelpers::parseNsCollectionRequired(dbName, cmdObj), cmdObj, false));

    auto placeholder = replaceEncryptedFieldsInFilter(*schemaTree, qr->getFilter());

    BSONObjBuilder bob;
    for (auto&& elem : cmdObj) {
        if (elem.fieldNameStringData() == "filter") {
            BSONObjBuilder filterBob = bob.subobjStart("filter");
            filterBob.appendElements(placeholder.result);
        } else {
            bob.append(elem);
        }
    }
    return {placeholder.hasEncryptionPlaceholders, placeholder.schemaRequiresEncryption, bob.obj()};
}

PlaceHolderResult addPlaceHoldersForAggregate(
    const std::string& dbName,
    const BSONObj& cmdObj,
    std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    return PlaceHolderResult();
}

PlaceHolderResult addPlaceHoldersForCount(const std::string& dbName,
                                          const BSONObj& cmdObj,
                                          std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    BSONObjBuilder resultBuilder;
    auto countCmd = CountCommand::parse(IDLParserErrorContext("count"), cmdObj);
    auto query = countCmd.getQuery();
    auto newQueryPlaceholder = replaceEncryptedFieldsInFilter(*schemaTree, query);
    countCmd.setQuery(newQueryPlaceholder.result);

    return PlaceHolderResult{newQueryPlaceholder.hasEncryptionPlaceholders,
                             newQueryPlaceholder.schemaRequiresEncryption ||
                                 schemaTree->containsEncryptedNode(),
                             countCmd.toBSON(cmdObj)};
}

PlaceHolderResult addPlaceHoldersForDistinct(const std::string& dbName,
                                             const BSONObj& cmdObj,
                                             std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    auto parsedDistinct = DistinctCommand::parse(IDLParserErrorContext("distinct"), cmdObj);

    // The distinct key is not allowed to be encrypted with a keyId which points to another field.
    if (auto keyMetadata =
            schemaTree->getEncryptionMetadataForPath(FieldRef(parsedDistinct.getKey()))) {
        uassert(51131,
                "The distinct key is not allowed to be marked for encryption with a non-UUID keyId",
                keyMetadata.get().getKeyId().get().type() !=
                    EncryptSchemaKeyId::Type::kJSONPointer);
    }

    PlaceHolderResult placeholder;
    if (auto query = parsedDistinct.getQuery()) {
        // Replace any encrypted fields in the query, and overwrite the original query from the
        // parsed command.
        placeholder = replaceEncryptedFieldsInFilter(*schemaTree, parsedDistinct.getQuery().get());
        parsedDistinct.setQuery(placeholder.result);
    }

    // Serialize the parsed distinct command. Passing the original command object to 'serialize()'
    // allows the IDL to merge generic fields which the command does not specifically handle.
    return PlaceHolderResult{placeholder.hasEncryptionPlaceholders,
                             placeholder.schemaRequiresEncryption,
                             parsedDistinct.serialize(cmdObj).body};
}

/**
 * Asserts that if _id is encrypted, it is provided explicitly. In addition, asserts
 * that no top level field in 'doc' has the value Timestamp(0,0). In both of those cases mongod
 * would usually generate a value so they cannot be encrypted here.
 */
void verifyNoGeneratedEncryptedFields(BSONObj doc, const EncryptionSchemaTreeNode& schemaTree) {
    uassert(51130,
            "_id must be explicitly provided when configured as encrypted",
            !schemaTree.getEncryptionMetadataForPath(FieldRef("_id")) || doc["_id"]);
    // Top level Timestamp(0, 0) is not allowed to be inserted because the server replaces it
    // with a different generated value. A nested Timestamp(0,0) does not have this issue.
    for (auto&& element : doc) {
        if (schemaTree.getEncryptionMetadataForPath(FieldRef(element.fieldNameStringData()))) {
            uassert(51129,
                    "A command that inserts cannot supply Timestamp(0, 0) for an encrypted"
                    "top-level field at path " +
                        element.fieldNameStringData(),
                    element.type() != BSONType::bsonTimestamp ||
                        element.timestamp() != Timestamp(0, 0));
        }
    }
}

PlaceHolderResult addPlaceHoldersForFindAndModify(
    const std::string& dbName,
    const BSONObj& cmdObj,
    std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    auto request(uassertStatusOK(FindAndModifyRequest::parseFromBSON(
        CommandHelpers::parseNsCollectionRequired(dbName, cmdObj), cmdObj)));

    if (request.isUpsert()) {
        verifyNoGeneratedEncryptedFields(request.getUpdateObj(), *schemaTree.get());
    }
    auto newQuery = replaceEncryptedFieldsInFilter(*schemaTree.get(), request.getQuery());
    auto newUpdate = replaceEncryptedFieldsInUpdate(*schemaTree.get(), request.getUpdateObj());

    if (newQuery.hasEncryptionPlaceholders || newUpdate.hasEncryptionPlaceholders) {
        request.setQuery(newQuery.result);
        request.setUpdateObj(newUpdate.result);
        return PlaceHolderResult{true, schemaTree->containsEncryptedNode(), request.toBSON(cmdObj)};
    } else {
        return PlaceHolderResult{false, schemaTree->containsEncryptedNode(), cmdObj};
    }
}

PlaceHolderResult addPlaceHoldersForInsert(const OpMsgRequest& request,
                                           std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    auto batch = InsertOp::parse(request);
    auto docs = batch.getDocuments();
    PlaceHolderResult retPlaceholder;
    std::vector<BSONObj> docVector;
    for (const BSONObj& doc : docs) {
        verifyNoGeneratedEncryptedFields(doc, *schemaTree.get());
        auto placeholderPair = replaceEncryptedFields(doc, schemaTree.get(), FieldRef(), doc);
        retPlaceholder.hasEncryptionPlaceholders =
            retPlaceholder.hasEncryptionPlaceholders || placeholderPair.hasEncryptionPlaceholders;
        docVector.push_back(placeholderPair.result);
    }
    batch.setDocuments(docVector);
    std::set<StringData> fieldNames = request.body.getFieldNames<std::set<StringData>>();
    fieldNames.insert("documents"_sd);
    retPlaceholder.result = removeExtraFields(fieldNames, batch.toBSON(request.body));
    retPlaceholder.schemaRequiresEncryption = schemaTree->containsEncryptedNode();
    return retPlaceholder;
}

PlaceHolderResult addPlaceHoldersForUpdate(const OpMsgRequest& request,
                                           std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    auto updateOp = UpdateOp::parse(request);
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(nullptr, nullptr));

    auto updates = updateOp.getUpdates();
    std::vector<write_ops::UpdateOpEntry> updateVector;
    PlaceHolderResult phr;

    for (auto&& update : updateOp.getUpdates()) {
        auto& updateMod = update.getU();
        uassert(ErrorCodes::NotImplemented,
                "Pipeline updates not yet supported on mongocryptd",
                updateMod.type() == write_ops::UpdateModification::Type::kClassic);
        if (update.getUpsert()) {
            verifyNoGeneratedEncryptedFields(updateMod.getUpdateClassic(), *schemaTree.get());
        }
        auto newFilter = replaceEncryptedFieldsInFilter(*schemaTree.get(), update.getQ());
        auto newUpdate =
            replaceEncryptedFieldsInUpdate(*schemaTree.get(), updateMod.getUpdateClassic());
        updateVector.push_back(write_ops::UpdateOpEntry(newFilter.result, newUpdate.result));
        phr.hasEncryptionPlaceholders = phr.hasEncryptionPlaceholders ||
            newUpdate.hasEncryptionPlaceholders || newFilter.hasEncryptionPlaceholders;
    }

    updateOp.setUpdates(updateVector);
    std::set<StringData> fieldNames = request.body.getFieldNames<std::set<StringData>>();
    fieldNames.insert("updates"_sd);
    phr.result = removeExtraFields(fieldNames, updateOp.toBSON(request.body));
    phr.schemaRequiresEncryption = schemaTree->containsEncryptedNode();
    return phr;
}

PlaceHolderResult addPlaceHoldersForDelete(const OpMsgRequest& request,
                                           std::unique_ptr<EncryptionSchemaTreeNode> schemaTree) {
    invariant(schemaTree);
    PlaceHolderResult placeHolderResult{};

    auto deleteRequest = write_ops::Delete::parse(IDLParserErrorContext("delete"), request);

    std::vector<write_ops::DeleteOpEntry> markedDeletes;
    for (auto&& op : deleteRequest.getDeletes()) {
        markedDeletes.push_back(op);
        auto& opToMark = markedDeletes.back();
        auto resultForOp = replaceEncryptedFieldsInFilter(*schemaTree, opToMark.getQ());
        placeHolderResult.hasEncryptionPlaceholders =
            placeHolderResult.hasEncryptionPlaceholders || resultForOp.hasEncryptionPlaceholders;
        opToMark.setQ(resultForOp.result);
    }

    deleteRequest.setDeletes(std::move(markedDeletes));
    std::set<StringData> fieldNames = request.body.getFieldNames<std::set<StringData>>();
    fieldNames.insert("deletes"_sd);
    placeHolderResult.result = removeExtraFields(fieldNames, deleteRequest.toBSON(request.body));
    placeHolderResult.schemaRequiresEncryption = schemaTree->containsEncryptedNode();
    return placeHolderResult;
}

void serializePlaceholderResult(const PlaceHolderResult& placeholder, BSONObjBuilder* builder) {
    builder->append(kHasEncryptionPlaceholders, placeholder.hasEncryptionPlaceholders);
    builder->append(kSchemaRequiresEncryption, placeholder.schemaRequiresEncryption);
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
    PlaceHolderResult(const std::string& dbName,
                      const BSONObj& cmdObj,
                      std::unique_ptr<EncryptionSchemaTreeNode> schemaTree);

void processQueryCommand(const std::string& dbName,
                         const BSONObj& cmdObj,
                         BSONObjBuilder* builder,
                         QueryProcessFunction func) {
    BSONObjBuilder stripped;
    auto schema = extractJSONSchema(cmdObj, &stripped);

    // Parse the JSON Schema to an encryption schema tree.
    auto schemaTree = EncryptionSchemaTreeNode::parse(schema);

    PlaceHolderResult placeholder = func(dbName, stripped.obj(), std::move(schemaTree));
    auto fieldNames = cmdObj.getFieldNames<std::set<StringData>>();
    placeholder.result = removeExtraFields(fieldNames, placeholder.result);

    serializePlaceholderResult(placeholder, builder);
}

}  // namespace

PlaceHolderResult replaceEncryptedFields(BSONObj doc,
                                         const EncryptionSchemaTreeNode* schema,
                                         FieldRef leadingPath,
                                         const boost::optional<BSONObj>& origDoc) {
    PlaceHolderResult res;
    res.result = replaceEncryptedFieldsRecursive(
        schema, doc, origDoc, &leadingPath, &res.hasEncryptionPlaceholders);
    return res;
}

void processFindCommand(const std::string& dbName, const BSONObj& cmdObj, BSONObjBuilder* builder) {
    processQueryCommand(dbName, cmdObj, builder, addPlaceHoldersForFind);
}

void processAggregateCommand(const std::string& dbName,
                             const BSONObj& cmdObj,
                             BSONObjBuilder* builder) {
    processQueryCommand(dbName, cmdObj, builder, addPlaceHoldersForAggregate);
}

void processDistinctCommand(const std::string& dbName,
                            const BSONObj& cmdObj,
                            BSONObjBuilder* builder) {
    processQueryCommand(dbName, cmdObj, builder, addPlaceHoldersForDistinct);
}

void processCountCommand(const std::string& dbName,
                         const BSONObj& cmdObj,
                         BSONObjBuilder* builder) {
    processQueryCommand(dbName, cmdObj, builder, addPlaceHoldersForCount);
}

void processFindAndModifyCommand(const std::string& dbName,
                                 const BSONObj& cmdObj,
                                 BSONObjBuilder* builder) {
    processQueryCommand(dbName, cmdObj, builder, addPlaceHoldersForFindAndModify);
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

BSONObj buildEncryptPlaceholder(BSONElement elem,
                                const EncryptionMetadata& metadata,
                                const boost::optional<BSONObj>& origDoc,
                                const boost::optional<const EncryptionSchemaTreeNode&> schema) {
    invariant(metadata.getAlgorithm());
    invariant(metadata.getKeyId());

    FleAlgorithmInt integerAlgorithm = metadata.getAlgorithm() == FleAlgorithmEnum::kDeterministic
        ? FleAlgorithmInt::kDeterministic
        : FleAlgorithmInt::kRandom;

    EncryptionPlaceholder marking(integerAlgorithm, EncryptSchemaAnyType(elem));

    if (integerAlgorithm == FleAlgorithmInt::kDeterministic) {
        invariant(metadata.getInitializationVector());
        marking.setInitializationVector(metadata.getInitializationVector().get());
    }

    auto keyId = metadata.getKeyId();
    if (keyId.get().type() == EncryptSchemaKeyId::Type::kUUIDs) {
        marking.setKeyId(keyId.get().uuids()[0]);
    } else {
        uassert(51093, "A non-static (JSONPointer) keyId is not supported.", origDoc);
        auto pointer = keyId->jsonPointer();
        auto resolvedKey = pointer.evaluate(origDoc.get());
        uassert(30017,
                "keyId pointer '" + pointer.toString() + "' cannot point to an encrypted field",
                !(schema->getEncryptionMetadataForPath(pointer.toFieldRef())));
        uassert(51114,
                "keyId pointer '" + pointer.toString() + "' must point to a field that exists",
                resolvedKey);
        uassert(51115,
                "keyId pointer '" + pointer.toString() +
                    "' cannot point to an object, array or CodeWScope",
                !resolvedKey.mayEncapsulate());
        uassert(30037,
                "keyId pointer '" + pointer.toString() +
                    "' cannot point to an already encrypted object",
                !(resolvedKey.type() == BSONType::BinData &&
                  resolvedKey.binDataType() == BinDataType::Encrypt));
        if (resolvedKey.type() == BSONType::BinData &&
            resolvedKey.binDataType() == BinDataType::newUUID) {
            marking.setKeyId(uassertStatusOK(UUID::parse(resolvedKey)));
        } else {
            EncryptSchemaAnyType keyAltName(resolvedKey);
            marking.setKeyAltName(keyAltName);
        }
    }

    // Serialize the placeholder to BSON.
    BSONObjBuilder bob;
    marking.serialize(&bob);
    auto markingObj = bob.done();

    // Encode the placeholder BSON as BinData (sub-type 6 for encryption). Prepend the sub-subtype
    // byte represent the intent-to-encrypt marking before the BSON payload.
    BufBuilder binDataBuffer;
    binDataBuffer.appendChar(FleBlobSubtype::IntentToEncrypt);
    binDataBuffer.appendBuf(markingObj.objdata(), markingObj.objsize());

    BSONObjBuilder binDataBob;
    binDataBob.appendBinData(
        elem.fieldNameStringData(), binDataBuffer.len(), BinDataType::Encrypt, binDataBuffer.buf());
    return binDataBob.obj();
}

}  // namespace mongo
