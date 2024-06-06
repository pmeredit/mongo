/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
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

    BSONObj generateFileHeader(Date_t ts,
                               StringData version,
                               StringData compressionMode,
                               StringData mac,
                               BSONObj keyStoreIdentifier,
                               const AuditKeyManager::WrappedKey& encryptedKey);

    BSONObj generateFileHeaderAuthenticatedData(Date_t ts, StringData version);
};

}  // namespace audit
}  // namespace mongo
