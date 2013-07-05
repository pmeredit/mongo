/**
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include "audit_event.h"
#include "mongo/logger/log_domain.h"

namespace mongo {
namespace audit {

    typedef mongo::logger::LogDomain<AuditEvent> AuditLogDomain;

    AuditLogDomain* getGlobalAuditLogDomain();
}  // namespace audit
}  // namespace mongo
