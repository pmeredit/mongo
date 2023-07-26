/**
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include "mongo/base/error_codes.h"
#include "mongo/db/audit_interface.h"
#include "mongo/db/client.h"

#include "audit_event_type.h"

namespace mongo::audit {

// Send the specified event to the audit log if the manager is enabled and approves the event. If
// overrideTenant is true, instead of grabbing the active tenant's tenantId from the opCtx, uses the
// given tenantId when creating the AuditEvent. Throws on failure, returns true if the event was
// logged, returns false otherwise.
template <typename EventType>
bool tryLogEvent(Client* client,
                 typename EventType::TypeArgT type,
                 AuditInterface::AuditEvent::Serializer serializer,
                 ErrorCodes::Error code,
                 bool overrideTenant = false,
                 const boost::optional<TenantId>& tenantId = boost::none);

// Send the specified event to the audit log.
// Throws a uassertStatusOK DBException on failure.
void logEvent(const AuditInterface::AuditEvent& event);

// Logs the event when data containing privileges is changed via direct access.
void logDirectAuthOperation(Client* client,
                            const NamespaceString& nss,
                            const BSONObj& doc,
                            StringData operation);

}  // namespace mongo::audit
