/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/bson/bsonobj.h"

namespace mongo {

/**
 * Returns true if one or more fields are marked with encrypt in a JSON schema.
 */
bool isEncryptionNeeded(const BSONObj& jsonSchema);

}  // namespace mongo
