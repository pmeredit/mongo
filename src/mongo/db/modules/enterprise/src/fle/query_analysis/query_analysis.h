/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "encryption_schema_tree.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/matcher/schema/encrypt_schema_types.h"
#include "mongo/rpc/op_msg.h"

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

/*
 * Returns a PlaceHolderResult containing a document with all fields that were marked with
 * 'encrypt' in 'schema' replaced with EncryptionPlaceholders.
 *
 * The 'leadingPath' will be treated as a prefix to any fields in 'doc'. For example, calling this
 * function with a leading path 'a' and document {b: 1, c: 1} will mark "b" or "c" for encryption if
 * the schema indicates that either "a.b" or "a.c" are encrypted respectively.
 * If the original document is passed in as 'origDoc' it will be used to resolve JSON Pointer
 * keyIds. If it is not passed in, will throw on pointer keyIds.
 */
PlaceHolderResult replaceEncryptedFields(BSONObj doc,
                                         const EncryptionSchemaTreeNode* schema,
                                         FieldRef leadingPath,
                                         const boost::optional<BSONObj>& origDoc);

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
void processFindCommand(const std::string& dbName, const BSONObj& cmdObj, BSONObjBuilder* builder);

void processAggregateCommand(const std::string& dbName,
                             const BSONObj& cmdObj,
                             BSONObjBuilder* builder);

void processDistinctCommand(const std::string& dbName,
                            const BSONObj& cmdObj,
                            BSONObjBuilder* builder);

void processCountCommand(const std::string& dbName, const BSONObj& cmdObj, BSONObjBuilder* builder);

void processFindAndModifyCommand(const std::string& dbName,
                                 const BSONObj& cmdObj,
                                 BSONObjBuilder* builder);

// Write Ops commands take document sequences so we process OpMsgRequest instead of BSONObj

void processInsertCommand(const OpMsgRequest& request, BSONObjBuilder* builder);

void processUpdateCommand(const OpMsgRequest& request, BSONObjBuilder* builder);

void processDeleteCommand(const OpMsgRequest& request, BSONObjBuilder* builder);

/**
 * Builds an EncryptionPlaceholder using 'elem' and 'metadata'. Returns a single element BSONObj
 * whose field name is the same as the field name from 'elem' and whose value is a BinData (sub-type
 * 6) representing the placeholder. If 'origDoc' is initialized, will try to resolve a jsonPointer
 * in metadata using that document. Throws if the pointer evaluates to EOO, an array, CodeWScope,
 * or an object.
 */
BSONObj buildEncryptPlaceholder(
    BSONElement elem,
    const EncryptionMetadata& metadata,
    const boost::optional<BSONObj>& origDoc = boost::none,
    const boost::optional<const EncryptionSchemaTreeNode&> schema = boost::none);

}  // namespace mongo
