/*
 *    Copyright (C) 2013 10gen Inc.
 */

/**
 * Utility  methods used in the auditing subsystem.
 */

#pragma once

#include "audit_event.h"
#include "mongo/base/status.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/util/assert_util.h"

namespace mongo {

class ClientBasic;

namespace audit {

/**
 * Initializes the given "envelope" based on information in "client", plus
 * the "actionType" and "result" codes.
 */
void initializeEnvelope(AuditEventEnvelope* envelope,
                        ClientBasic* client,
                        ActionType actionType,
                        ErrorCodes::Error result);

/**
 * Returns an AuditEventEnvelope initialized with information from "client", "actionType" and
 * "result".
 */
inline AuditEventEnvelope makeEnvelope(ClientBasic* client,
                                       ActionType actionType,
                                       ErrorCodes::Error result) {
    AuditEventEnvelope envelope;
    initializeEnvelope(&envelope, client, actionType, result);
    return envelope;
}

}  // namespace audit
}  // namespace mongo
