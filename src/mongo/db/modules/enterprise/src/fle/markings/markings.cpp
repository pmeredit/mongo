/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#include "markings.h"

#include "mongo/bson/bsonobj.h"

namespace mongo {

// TODO - Query implements this correctly
bool isEncryptionNeeded(const BSONObj& jsonSchema) {
    return false;
}

}  // namespace mongo
