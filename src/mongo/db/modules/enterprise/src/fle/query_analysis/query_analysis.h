/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/bson/bsonobj.h"
#include "mongo/rpc/op_msg.h"

namespace mongo {

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


}  // namespace mongo
