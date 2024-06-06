/*
 *    Copyright (C) 2013-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <string>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"

namespace mongo {
namespace audit {

Status validateAuditLogDestination(const std::string& dest);
Status validateAuditLogFormat(const std::string& format);
Status validateAuditLogSchema(const std::string& strSchema);

enum class AuditSchema {
    kMongo,
    kOCSF,
};

static StatusWith<AuditSchema> parseAuditSchema(StringData schema) {
    if (schema == "mongo"_sd) {
        return AuditSchema::kMongo;
    } else if (schema == "OCSF"_sd) {
        return AuditSchema::kOCSF;
    } else {
        return {ErrorCodes::BadValue, "auditSchema must be one of 'mongo' or 'OCSF'"};
    }
}

}  // namespace audit
}  // namespace mongo
