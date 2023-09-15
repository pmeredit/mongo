/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/transport/client_transport_observer.h"
#include "mongo/transport/service_entry_point_impl.h"
#include "mongo/transport/session_manager_common.h"

namespace mongo {

class ServiceEntryPointCryptD final : public ServiceEntryPointImpl {
public:
    using ServiceEntryPointImpl::ServiceEntryPointImpl;

    Future<DbResponse> handleRequest(OperationContext* opCtx,
                                     const Message& request) noexcept final;
};

class ClientObserverCryptD final : public transport::ClientTransportObserver {
public:
    void onClientConnect(Client*) final;
};

}  // namespace mongo
