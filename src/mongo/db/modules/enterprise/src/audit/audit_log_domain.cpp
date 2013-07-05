/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_log_domain.h"

#include "mongo/logger/log_domain-impl.h"

namespace mongo {

namespace logger {
    template class LogDomain<audit::AuditEvent>;
}  // namespace logger

namespace audit {

    static AuditLogDomain globalAuditLogDomain;

    AuditLogDomain* getGlobalAuditLogDomain() { return &globalAuditLogDomain; }

}  // namespace audit
}  // namespace mongo
