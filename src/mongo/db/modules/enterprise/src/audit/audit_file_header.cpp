/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include "audit_file_header.h"

#include <string>

#include "audit/audit_header_options_gen.h"
#include <mongo/util/time_support.h>

namespace mongo {
namespace audit {

AuditFileHeader::AuditFileHeader() = default;

BSONObj AuditFileHeader::generateFileHeader(StringData version,
                                            StringData compressionMode,
                                            BSONObj keyStoreIdentifier,
                                            const AuditKeyManager::WrappedKey& encryptedKey) {
    Date_t ts = Date_t::now();

    AuditHeaderOptionsDocument opts;
    opts.setTs(ts);
    opts.setVersion(version);
    opts.setCompressionMode(compressionMode);
    opts.setKeyStoreIdentifier(keyStoreIdentifier);
    opts.setEncryptedKey(encryptedKey);
    return opts.toBSON();
}

}  // namespace audit
}  // namespace mongo
