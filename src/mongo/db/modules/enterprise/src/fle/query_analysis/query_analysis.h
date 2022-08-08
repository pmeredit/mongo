/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/bson/bsonobj.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/matcher/schema/encrypt_schema_types.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/collation/collator_interface.h"
#include "mongo/rpc/op_msg.h"
#include "mongo/util/overloaded_visitor.h"
#include "resolved_encryption_info.h"

namespace mongo {

class EncryptionSchemaTreeNode;

namespace query_analysis {

constexpr auto kJsonSchema = "jsonSchema"_sd;
constexpr auto kIsRemoteSchema = "isRemoteSchema"_sd;

constexpr auto kEncryptionInformation = "encryptionInformation"_sd;

/**
 * Struct to hold information about placeholder results returned to client.
 */
struct PlaceHolderResult {
    // Set to true if 'result' contains an intent-to-encrypt marking.
    bool hasEncryptionPlaceholders{false};

    // Set to true if the JSON Schema contains a field which should be marked for encryption.
    bool schemaRequiresEncryption{false};

    // MatchExpression representation of filter after intent-to-encrypt marking and before
    // serialization. Sometimes null since not all analyzed query components involve a
    // MatchExpression. This is an intermediate result only and not returned directly to the
    // client.
    std::unique_ptr<MatchExpression> matchExpr;

    // Serialized command result after replacing fields with their appropriate intent-to-encrypt
    // marking.
    BSONObj result;
};

/**
 * 'kRemote' represents the validation schema from mongod and 'kLocal' represents the schema
 * generated using the drivers.
 */
enum class EncryptionSchemaType { kRemote, kLocal };

struct QueryAnalysisParams {
    QueryAnalysisParams(const BSONObj& jsonSchema,
                        const EncryptionSchemaType schemaType,
                        BSONObj strippedObj)
        : schema(FLE1Params{jsonSchema, schemaType}), strippedObj(std::move(strippedObj)) {}

    QueryAnalysisParams(const BSONObj& encryptionInfo, BSONObj strippedObj)
        : schema(FLE2Params{encryptionInfo}), strippedObj(std::move(strippedObj)) {}

    struct FLE1Params {
        BSONObj jsonSchema;
        EncryptionSchemaType schemaType;
    };
    struct FLE2Params {
        BSONObj encryptedFieldsConfig;
    };

    FleVersion fleVersion() const {
        return stdx::visit(
            OverloadedVisitor{
                [](const FLE1Params&) { return FleVersion::kFle1; },
                [](const FLE2Params&) { return FleVersion::kFle2; },
            },
            schema);
    }

    stdx::variant<FLE1Params, FLE2Params> schema;


    /**
     * Command object without the encryption-related fields.
     */
    BSONObj strippedObj;
};

/**
 * Serialize a placeholder result to BSON.
 */
void serializePlaceholderResult(const PlaceHolderResult& placeholder, BSONObjBuilder* builder);

/**
 * Deserialize BSON to a placeholder result.
 */
PlaceHolderResult parsePlaceholderResult(BSONObj obj);

/**
 * Indicates whether we are creating intent-to-encrypt placeholders in the context of performing
 * equality comparisons to encrypted fields or in the context of writing encrypted data.
 *
 * Additional validity checking is necessary for placeholders in a comparison context. For example,
 * it is only legal for a query to check for equality to an encrypted field if the deterministic
 * encryption algorithm is used. When creating encrypted data through insert or update, however, the
 * encryption algorithm may be either deterministic or random. Similarly, restrictions around
 * collation are only relevant in the context of comparison.
 */
enum class EncryptionPlaceholderContext {
    kComparison,
    kWrite,
};

/*
 * Returns a PlaceHolderResult containing a document with all fields that were marked with
 * 'encrypt' in 'schema' replaced with EncryptionPlaceholders.
 *
 * The EncryptionPlaceholderContext communicates which set of validity checks should apply to any
 * intent-to-encrypt markings produced by this function. If 'doc' represents data that will be
 * written to mongod, callers must pass the 'kWrite' EncryptionPlaceholderContext. If 'doc' contains
 * a constant against which we're making a comparison in a query, then callers should pass the
 * 'kComparison' context.
 *
 * The 'leadingPath' will be treated as a prefix to any fields in 'doc'. For example, calling this
 * function with a leading path 'a' and document {b: 1, c: 1} will mark "b" or "c" for encryption if
 * the schema indicates that either "a.b" or "a.c" are encrypted respectively.
 * If the original document is passed in as 'origDoc' it will be used to resolve JSON Pointer
 * keyIds. If it is not passed in, will throw on pointer keyIds.
 *
 * If the 'placeholderContext' is 'kComparison', callers must pass the appropriate 'collator'. This
 * function will throw an assertion if a collation-aware comparison would be required against an
 * encrypted field. The 'collator' is ignored in the 'kWrite' context.
 */
PlaceHolderResult replaceEncryptedFields(BSONObj doc,
                                         const EncryptionSchemaTreeNode* schema,
                                         EncryptionPlaceholderContext placeholderContext,
                                         FieldRef leadingPath,
                                         const boost::optional<BSONObj>& origDoc,
                                         const CollatorInterface* collator);

/**
 * Process a find command and return the result with placeholder information.
 *
 * Returns:
 * {
 *   hasEncryptionPlaceholders : <true/false>,
 *   schemaRequiresEncryption: <true/false>,
 *   result : {
 *     filter : {...}
 *     $db : cmdObj[$db]
 *   }
 * }
 */
void processFindCommand(OperationContext* opCtx,
                        const std::string& dbName,
                        const BSONObj& cmdObj,
                        BSONObjBuilder* builder,
                        NamespaceString ns);

void processAggregateCommand(OperationContext* opCtx,
                             const std::string& dbName,
                             const BSONObj& cmdObj,
                             BSONObjBuilder* builder,
                             NamespaceString ns);

void processDistinctCommand(OperationContext* opCtx,
                            const std::string& dbName,
                            const BSONObj& cmdObj,
                            BSONObjBuilder* builder,
                            NamespaceString ns);

void processCountCommand(OperationContext* opCtx,
                         const std::string& dbName,
                         const BSONObj& cmdObj,
                         BSONObjBuilder* builder,
                         NamespaceString ns);

void processFindAndModifyCommand(OperationContext* opCtx,
                                 const std::string& dbName,
                                 const BSONObj& cmdObj,
                                 BSONObjBuilder* builder,
                                 NamespaceString ns);

void processCreateCommand(OperationContext* opCtx,
                          const std::string& dbName,
                          const BSONObj& cmdObj,
                          BSONObjBuilder* builder,
                          NamespaceString ns);

void processCollModCommand(OperationContext* opCtx,
                           const std::string& dbName,
                           const BSONObj& cmdObj,
                           BSONObjBuilder* builder,
                           NamespaceString ns);

void processCreateIndexesCommand(OperationContext* opCtx,
                                 const std::string& dbName,
                                 const BSONObj& cmdObj,
                                 BSONObjBuilder* builder,
                                 NamespaceString ns);

// Write Ops commands take document sequences so we process OpMsgRequest instead of BSONObj

void processInsertCommand(OperationContext* opCtx,
                          const OpMsgRequest& request,
                          BSONObjBuilder* builder,
                          NamespaceString ns);

void processUpdateCommand(OperationContext* opCtx,
                          const OpMsgRequest& request,
                          BSONObjBuilder* builder,
                          NamespaceString ns);

void processDeleteCommand(OperationContext* opCtx,
                          const OpMsgRequest& request,
                          BSONObjBuilder* builder,
                          NamespaceString ns);

/**
 * Builds an EncryptionPlaceholder using 'input' and 'metadata'. Returns a Value which is a BinData
 * (sub-type 6) representing the placeholder.
 *
 * Assumes that the 'input' is being used in a comparison context.
 */
Value buildEncryptPlaceholder(Value input,
                              const ResolvedEncryptionInfo& metadata,
                              EncryptionPlaceholderContext placeholderContext,
                              const CollatorInterface* collator);

/**
 * Builds an EncryptionPlaceholder using 'elem' and 'metadata'. Returns a single element BSONObj
 * whose field name is the same as the field name from 'elem' and whose value is a BinData (sub-type
 * 6) representing the placeholder.
 *
 * The EncryptionPlaceholderContext communicates which set of validity checks should apply to the
 * intent-to-encrypt marking produced by this function. If 'elem' represents data that will be
 * written to mongod, callers must pass the 'kWrite' EncryptionPlaceholderContext. Conversely, if
 * 'elem' is a constant against which we're making a comparison in a query, then callers should pass
 * the 'kComparison' context.
 *
 * If the 'placeholderContext' is 'kComparison', callers must pass the appropriate 'collator'. This
 * function will throw an assertion if a collation-aware comparison would be required against an
 * encrypted field. The 'collator' is ignored in the 'kWrite' context.
 *
 * If 'origDoc' is non-none, will try to resolve a jsonPointer in metadata using that document.
 * Throws if the pointer evaluates to EOO, an array, CodeWScope, or an object.
 */
BSONObj buildEncryptPlaceholder(
    BSONElement elem,
    const ResolvedEncryptionInfo& metadata,
    EncryptionPlaceholderContext placeholderContext,
    const CollatorInterface* collator,
    const boost::optional<BSONObj>& origDoc = boost::none,
    boost::optional<const EncryptionSchemaTreeNode&> schema = boost::none);

/**
 * Get the range spec from an encrypted range placeholder. The returned elements are held by the
 * passed-in placeholder, so the placeholder must outlive the return value of this function.
 */
FLE2RangeSpec getEncryptedRange(const FLE2EncryptionPlaceholder& placeholder);

/**
 * Serialize a FLE2EncryptionPlaceholder to BSON, properly wrapping the placeholder as bindata with
 * the encryption subtype.
 */
BSONObj serializeFle2Placeholder(StringData fieldname,
                                 const FLE2EncryptionPlaceholder& placeholder);

/**
 * Build a $encryptedBetween MatchExpression with a placeholder range. The min and max BSONElements
 * will be copied into owned BSON inside the created MatchExpression.
 */
std::unique_ptr<EncryptedBetweenMatchExpression> buildEncryptedBetweenWithPlaceholder(
    StringData fieldname,
    UUID ki,
    int64_t cm,
    int32_t sparsity,
    std::pair<BSONElement, bool> minSpec,
    std::pair<BSONElement, bool> maxSpec);

/**
 * Parse a range placeholder from an BinData payload.
 */
FLE2EncryptionPlaceholder parseRangePlaceholder(BSONElement elt);

}  // namespace query_analysis
}  // namespace mongo
