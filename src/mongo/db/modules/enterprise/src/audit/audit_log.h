/**
 *    Copyright (C) 2013-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/base/error_codes.h"
#include "mongo/db/audit_interface.h"
#include "mongo/db/client.h"

#include "audit_event_type.h"

namespace mongo::audit {

// Send the specified event to the audit log if the manager is enabled and approves the event. If
// given tenantId exists, instead of grabbing the active tenant's tenantId from the opCtx, uses the
// given tenantId when creating the AuditEvent. Throws on failure, returns true if the event was
// logged, returns false otherwise.
template <typename EventType>
bool tryLogEvent(typename EventType::TypeArgT tryLogParams);

// Send the specified event to the audit log.
// Throws a uassertStatusOK DBException on failure.
void logEvent(const AuditInterface::AuditEvent& event);

// Type of direct mutation to the authz collection which was performed
// when calling Audit(Mongo|OCSF)::logDirectAuthOperation().
enum class DirectAuthOperation {
    kCreate,
    kDrop,
    kInsert,
    kRemove,
    kRename,
    kUpdate,
};

void sanitizeCredentialsAuditDoc(BSONObjBuilder* builder, const BSONObj& doc);

bool isStandaloneOrPrimary(OperationContext* opCtx);

bool isDDLAuditingAllowed(Client* client,
                          const NamespaceString& nsname,
                          boost::optional<const NamespaceString&> renameTarget = boost::none);

/**
 * Do not use this function outside of unittests. This will invariant if
 * the format is not AuditFormatMock.
 */
BSONObj getLastLine_forTest();

}  // namespace mongo::audit
