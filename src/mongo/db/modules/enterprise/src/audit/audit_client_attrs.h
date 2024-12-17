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

class AuditUserAttrs {
public:
    AuditUserAttrs(boost::optional<UserName> username, std::vector<RoleName> rolenames)
        : username(std::move(username)), rolenames(std::move(rolenames)){};

    static AuditUserAttrs* get(OperationContext* opCtx);
    static void set(OperationContext* opCtx, std::unique_ptr<AuditUserAttrs> attrs);

    boost::optional<UserName> username;
    std::vector<RoleName> rolenames;
};

class AuditClientObserver final : public ServiceContext::ClientObserver {
public:
    void onCreateClient(Client* client) final{};
    void onDestroyClient(Client* client) final{};

    void onCreateOperationContext(OperationContext* opCtx) final;

    void onDestroyOperationContext(OperationContext* opCtx) final{};
};

}  // namespace mongo::audit
