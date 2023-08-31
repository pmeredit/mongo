/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/transport/service_entry_point_impl.h"
#include "mongo/transport/session_manager_common.h"

namespace mongo {

class ServiceEntryPointCryptD final : public ServiceEntryPointImpl {
public:
    using ServiceEntryPointImpl::ServiceEntryPointImpl;

    Future<DbResponse> handleRequest(OperationContext* opCtx,
                                     const Message& request) noexcept final;
};

class SessionManagerCryptD final : public transport::SessionManagerCommon {
public:
    using transport::SessionManagerCommon::SessionManagerCommon;

    void startSession(std::shared_ptr<transport::Session> session) final;
};

}  // namespace mongo
