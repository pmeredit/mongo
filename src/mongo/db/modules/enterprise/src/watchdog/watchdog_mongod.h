/**
 * Copyright (C) 2017 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/base/status.h"

namespace mongo {

/**
* Start the watchdog.
*/
void startWatchdog();

/**
 * Callbacks used by the 'watchdogPeriodSeconds' set parameter.
 */
Status validateWatchdogPeriodSeconds(const int& value);
Status onUpdateWatchdogPeriodSeconds(const int& value);

}  // namespace mongo
