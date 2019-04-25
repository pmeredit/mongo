/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "encryption_schema_tree.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/matcher/schema/encrypt_schema_types.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/value.h"
#include "mongo/db/query/collation/collator_interface.h"
#include "mongo/rpc/op_msg.h"
#include "resolved_encryption_info.h"

namespace mongo {

/**
 * Struct to hold information about placeholder results returned to client.
 */
struct PlaceHolderResult {
    // Set to true if 'result' contains an intent-to-encrypt marking.
    bool hasEncryptionPlaceholders{false};

    // Set to true if the JSON Schema contains a field which should be marked for encryption.
    bool schemaRequiresEncryption{false};

    // Serialized command result after replacing fields with their appropriate intent-to-encrypt
    // marking.
    BSONObj result;
};

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
                        BSONObjBuilder* builder);

void processAggregateCommand(OperationContext* opCtx,
                             const std::string& dbName,
                             const BSONObj& cmdObj,
                             BSONObjBuilder* builder);

void processDistinctCommand(OperationContext* opCtx,
                            const std::string& dbName,
                            const BSONObj& cmdObj,
                            BSONObjBuilder* builder);

void processCountCommand(OperationContext* opCtx,
                         const std::string& dbName,
                         const BSONObj& cmdObj,
                         BSONObjBuilder* builder);

void processFindAndModifyCommand(OperationContext* opCtx,
                                 const std::string& dbName,
                                 const BSONObj& cmdObj,
                                 BSONObjBuilder* builder);

// Write Ops commands take document sequences so we process OpMsgRequest instead of BSONObj

void processInsertCommand(OperationContext* opCtx,
                          const OpMsgRequest& request,
                          BSONObjBuilder* builder);

void processUpdateCommand(OperationContext* opCtx,
                          const OpMsgRequest& request,
                          BSONObjBuilder* builder);

void processDeleteCommand(OperationContext* opCtx,
                          const OpMsgRequest& request,
                          BSONObjBuilder* builder);

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
    const boost::optional<const EncryptionSchemaTreeNode&> schema = boost::none);

}  // namespace mongo
