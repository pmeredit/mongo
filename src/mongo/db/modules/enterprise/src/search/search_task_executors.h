/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <memory>

#include "mongo/executor/task_executor.h"

namespace mongo {

class ServiceContext;

namespace executor {

/**
 * Provides access to a service context scoped task executor for mongot.
 */
std::shared_ptr<TaskExecutor> getMongotTaskExecutor(ServiceContext* svc);

/**
 * Provides access to a service context scoped task executor for the search-index-management server.
 */
std::shared_ptr<TaskExecutor> getSearchIndexManagementTaskExecutor(ServiceContext* svc);

}  // namespace executor
}  // namespace mongo
