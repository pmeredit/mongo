/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/service_context.h"
#include "mongo/transport/service_entry_point.h"
#include "mongo/util/time_support.h"

namespace mongo {

/**
 * Start the idle watchdog.
 */
void startIdleWatchdog(ServiceContext* serviceContext,
                       Seconds seconds,
                       ServiceEntryPoint* entryPoint);

/**
 * Signal the idle watchdog to postpone shutdown because a new connection was made.
 */
void signalIdleWatchdog();

/**
 * Shutdown the idle watchdog.
 */
void shutdownIdleWatchdog(ServiceContext* serviceContext);

}  // namespace mongo
