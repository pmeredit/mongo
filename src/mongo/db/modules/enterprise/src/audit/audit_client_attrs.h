/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <vector>

#include <boost/optional.hpp>

#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/operation_context.h"

namespace mongo::audit {

class AuditClientAttrs {
public:
    AuditClientAttrs(boost::optional<UserName> username, std::vector<RoleName> rolenames)
        : username(std::move(username)), rolenames(std::move(rolenames)){};

    static AuditClientAttrs* get(OperationContext* opCtx);
    static void set(OperationContext* opCtx, std::unique_ptr<AuditClientAttrs> attrs);

    boost::optional<UserName> username;
    std::vector<RoleName> rolenames;
};

}  // namespace mongo::audit
