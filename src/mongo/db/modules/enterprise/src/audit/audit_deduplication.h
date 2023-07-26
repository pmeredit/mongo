/**
 *    Copyright (C) 2023 10gen Inc.
 */

#pragma once

#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "mongo/db/client.h"

namespace mongo::audit {

// Instance of this class should be attached to OperationContext during regular user management
// functions. When a CRUD operation is detected on one of privilege collections and context
// is decorated with said instance, consider it a duplicate event and pass
template <typename EventType>
class AuditDeduplication {
public:
    static const OperationContext::Decoration<AuditDeduplication> get;

    static bool wasOperationAlreadyAudited(Client* client) {
        const AuditDeduplication& ad(AuditDeduplication::get(client->getOperationContext()));
        return ad._auditIsDone;
    }

    static void markOperationAsAudited(Client* client) {
        AuditDeduplication& ad(AuditDeduplication::get(client->getOperationContext()));
        ad._auditIsDone = true;
    }

    static void tryAuditEventAndMark(Client* client,
                                     typename EventType::TypeArgT type,
                                     AuditInterface::AuditEvent::Serializer serializer,
                                     ErrorCodes::Error code) {
        const auto wasLogged = tryLogEvent<EventType>(client, type, std::move(serializer), code);
        if (wasLogged) {
            markOperationAsAudited(client);
        }
    }

private:
    bool _auditIsDone = false;
};

template <typename EventType>
const OperationContext::Decoration<AuditDeduplication<EventType>>
    AuditDeduplication<EventType>::get =
        OperationContext::declareDecoration<AuditDeduplication<EventType>>();

}  // namespace mongo::audit
