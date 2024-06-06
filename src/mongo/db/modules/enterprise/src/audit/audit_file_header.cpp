/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit_file_header.h"

#include <string>

#include "audit/audit_header_options_gen.h"
#include "mongo/util/time_support.h"

namespace mongo {
namespace audit {

AuditFileHeader::AuditFileHeader() = default;

BSONObj AuditFileHeader::generateFileHeader(Date_t ts,
                                            StringData version,
                                            StringData compressionMode,
                                            StringData mac,
                                            BSONObj keyStoreIdentifier,
                                            const AuditKeyManager::WrappedKey& encryptedKey) {
    AuditHeaderOptionsDocument opts;
    opts.setTs(ts);
    opts.setVersion(version);
    opts.setCompressionMode(compressionMode);
    opts.setKeyStoreIdentifier(keyStoreIdentifier);
    opts.setEncryptedKey(encryptedKey);
    opts.setMAC(mac);
    return opts.toBSON();
}

BSONObj AuditFileHeader::generateFileHeaderAuthenticatedData(Date_t ts, StringData version) {
    return BSON(AuditHeaderOptionsDocument::kTsFieldName
                << ts << AuditHeaderOptionsDocument::kVersionFieldName << version);
}

}  // namespace audit
}  // namespace mongo
