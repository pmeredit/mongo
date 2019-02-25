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
    bool hasEncryptionPlaceholders{false};

    BSONObj result;
};

/*
 * Returns a PlaceHolderResult containing a document with all fields that were marked with
 * 'encrypt' in 'schema' replaced with EncryptionPlaceholders.
 */
PlaceHolderResult replaceEncryptedFields(BSONObj doc, const EncryptionSchemaTreeNode* schema);

/**
 * Returns true if one or more fields are marked with 'encrypt' in a JSON schema.
 *
 * Throws an error on invalid schemas.
 */
bool isEncryptionNeeded(const BSONObj& jsonSchema);

/**
 * Process a find command and return the result with placeholder information.
 *
 * Returns:
 * {
 *   hasEncryptionPlaceholders : <true/false>
 *   result : {
 *     filter : {...}
 *     $db : cmdObj[$db]
 *   }
 * }
 */
void processFindCommand(const BSONObj& cmdObj, BSONObjBuilder* builder);

void processAggregateCommand(const BSONObj& cmdObj, BSONObjBuilder* builder);

void processDistinctCommand(const BSONObj& cmdObj, BSONObjBuilder* builder);

void processCountCommand(const BSONObj& cmdObj, BSONObjBuilder* builder);

void processFindAndModifyCommand(const BSONObj& cmdObj, BSONObjBuilder* builder);


// Write Ops commands take document sequences so we process OpMsgRequest instead of BSONObj

void processInsertCommand(const OpMsgRequest& request, BSONObjBuilder* builder);

void processUpdateCommand(const OpMsgRequest& request, BSONObjBuilder* builder);

void processDeleteCommand(const OpMsgRequest& request, BSONObjBuilder* builder);

/**
 * Builds an EncryptionPlaceholder using 'elem' and 'metadata'. Returns a single element BSONObj
 * whose field name is the same as the field name from 'elem' and whose value is a BinData (sub-type
 * 6) representing the placeholder.
 */
BSONObj buildEncryptPlaceholder(BSONElement elem, const EncryptionMetadata& metadata);

}  // namespace mongo
