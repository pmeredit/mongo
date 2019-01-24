/*
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include <atomic>
#include <string>
#include <vector>

#include "audit_manager.h"  // AuditFormat
#include "mongo/base/status.h"
#include "mongo/db/jsobj.h"

namespace mongo {
namespace audit {

struct AuditGlobalParams {
    bool enabled;
    BSONObj auditFilter;
    std::string auditPath;
    AtomicWord<bool> auditAuthorizationSuccess;

    AuditFormat auditFormat;

    AuditGlobalParams() : enabled(false), auditFormat(AuditFormatJsonFile) {}
};

extern AuditGlobalParams auditGlobalParams;

Status validateAuditLogDestination(const std::string& dest);
Status validateAuditLogFormat(const std::string& format);

}  // namespace audit
}  // namespace mongo
