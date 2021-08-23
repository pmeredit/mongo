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
                                            StringData kmipKeyStoreIdentifier,
                                            StringData kmipEncryptionKeyIdentifier) {
    Date_t ts = Date_t::now();

    AuditHeaderOptionsDocument opts =
        AuditHeaderOptionsDocument(ts,
                                   version.toString(),
                                   compressionMode.toString(),
                                   kmipKeyStoreIdentifier.toString(),
                                   kmipEncryptionKeyIdentifier.toString());

    return opts.toBSON();
}

}  // namespace audit
}  // namespace mongo
