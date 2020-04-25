/**
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include "audit_event.h"

namespace mongo::audit {

// Send the specified event to the audit log.
// Throws a uassertStatusOK DBException on failure.
void logEvent(const AuditEvent& event);

}  // namespace mongo::audit
