/**
 * Copyright (C) 2017 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <cstdint>

namespace mongo {

constexpr std::int32_t watchdogPeriodSecondsDefault = -1;

/**
* Start the watchdog.
*/
void startWatchdog();

}  // namespace mongo
