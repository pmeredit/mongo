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

    DbResponse handleRequest(OperationContext* opCtx, const Message& request) final;
};

}  // namespace mongo
