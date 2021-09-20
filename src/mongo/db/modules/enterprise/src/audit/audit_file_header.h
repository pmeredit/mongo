/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#pragma once

#include "audit_format.h"
#include "audit_key_manager.h"

#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"

namespace mongo {
namespace audit {

/**
 * Contains functions for the AuditEncryptionCompressionManager
 * to call to generate a file header.
 */
class AuditFileHeader {
public:
    AuditFileHeader();

    BSONObj generateFileHeader(StringData version,
                               StringData compressionMode,
                               BSONObj keyStoreIdentifier,
                               const AuditKeyManager::WrappedKey& encryptedKey);
};

}  // namespace audit
}  // namespace mongo
