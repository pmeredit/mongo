/**
 *  Copyright (C) 2020 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "audit/audit_event_type_gen.h"

namespace mongo {
namespace audit {

using AuditEventType = AuditEventTypeEnum;
constexpr inline size_t kNumAuditEventTypes = idlEnumCount<AuditEventTypeEnum>;

}  // namespace audit
}  // namespace mongo
