/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/executor/pinned_connection_task_executor.h"
#include "mongo/executor/task_executor.h"

namespace mongo {

class ServiceContext;

namespace executor {

/**
 * Provides access to a service context scoped task executor for mongot.
 */
TaskExecutor* getMongotTaskExecutor(ServiceContext* svc);

std::shared_ptr<PinnedConnectionTaskExecutor> makePinnedConnectionTaskExecutor(ServiceContext* svc);

}  // namespace executor
}  // namespace mongo
