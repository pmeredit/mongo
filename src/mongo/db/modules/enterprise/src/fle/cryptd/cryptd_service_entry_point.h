/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/transport/service_entry_point_impl.h"

namespace mongo {

class ServiceEntryPointCryptD final : public ServiceEntryPointImpl {
public:
    explicit ServiceEntryPointCryptD(ServiceContext* svcCtx) : ServiceEntryPointImpl(svcCtx) {}

    void startSession(transport::SessionHandle session) final;

    Future<DbResponse> handleRequest(OperationContext* opCtx,
                                     const Message& request) noexcept final;
};

}  // namespace mongo
