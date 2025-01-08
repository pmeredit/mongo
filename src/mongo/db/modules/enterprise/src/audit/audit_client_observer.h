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
class AuditClientObserver final : public ServiceContext::ClientObserver {
public:
    void onCreateClient(Client* client) final{};
    void onDestroyClient(Client* client) final{};

    void onCreateOperationContext(OperationContext* opCtx) final;
    void onDestroyOperationContext(OperationContext* opCtx) final{};
};

}  // namespace mongo::audit
