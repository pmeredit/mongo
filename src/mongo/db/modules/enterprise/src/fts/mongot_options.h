/**
 * Copyright (C) 2019-present MongoDB, Inc.
 */

#pragma once

#include <string>

#include "mongo/base/status.h"

namespace mongo {

/**
 * Mongot (FTS) configuration options
 */
struct MongotParams {
    static Status onSetHost(const std::string&);
    static Status onValidateHost(StringData str);

    MongotParams();

    bool enabled = false;
    std::string host;
};

extern MongotParams globalMongotParams;

}  // namespace mongo
