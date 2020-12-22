/**
 *  Copyright (C) 2020 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <array>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"

namespace mongo {

#define EXPAND_AUDIT_EVENT_TYPE(X) \
    X(authenticate)                \
    X(authCheck)                   \
    X(applicationMessage)          \
    X(createIndex)                 \
    X(createCollection)            \
    X(importCollection)            \
    X(createDatabase)              \
    X(dropIndex)                   \
    X(dropCollection)              \
    X(dropDatabase)                \
    X(renameCollection)            \
    X(replSetReconfig)             \
    X(grantRolesToUser)            \
    X(revokeRolesFromUser)         \
    X(createRole)                  \
    X(updateRole)                  \
    X(dropRole)                    \
    X(dropAllRolesFromDatabase)    \
    X(grantRolesToRole)            \
    X(revokeRolesFromRole)         \
    X(grantPrivilegesToRole)       \
    X(revokePrivilegesFromRole)    \
    X(enableSharding)              \
    X(addShard)                    \
    X(shardCollection)             \
    X(removeShard)                 \
    X(refineCollectionShardKey)    \
    X(shutdown)                    \
    X(startup)                     \
    X(logout)                      \
    X(createUser)                  \
    X(dropUser)                    \
    X(dropAllUsersFromDatabase)    \
    X(updateUser)


enum class AuditEventType : uint32_t {
#define X_(a) a,
    EXPAND_AUDIT_EVENT_TYPE(X_)
#undef X_
};

#define X_(a) +1  // just count them
static constexpr uint32_t kNumAuditEventTypes = 0 EXPAND_AUDIT_EVENT_TYPE(X_);
#undef X_

StatusWith<AuditEventType> parseAuditEventFromString(StringData event);
StringData toStringData(AuditEventType a);
std::string toString(AuditEventType a);
std::ostream& operator<<(std::ostream& os, const AuditEventType& a);

}  // namespace mongo