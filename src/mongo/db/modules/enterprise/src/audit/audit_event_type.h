/**
 *  Copyright (C) 2020-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "audit/audit_event_type_gen.h"

namespace mongo {
namespace audit {

using AuditEventType = AuditEventTypeEnum;
constexpr inline size_t kNumAuditEventTypes = idlEnumCount<AuditEventTypeEnum>;

}  // namespace audit
}  // namespace mongo
