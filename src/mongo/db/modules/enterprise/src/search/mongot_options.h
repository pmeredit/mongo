/**
 * Copyright (C) 2019-present MongoDB, Inc.
 */

#pragma once

#include <boost/optional.hpp>
#include <string>

#include "mongo/base/status.h"
#include "mongo/db/tenant_id.h"

namespace mongo {

/**
 * Mongot (search) configuration options
 */
struct MongotParams {
    static Status onSetHost(const std::string&);
    static Status onValidateHost(StringData str, const boost::optional<TenantId>&);

    MongotParams();

    bool enabled = false;
    std::string host;
};

extern MongotParams globalMongotParams;

}  // namespace mongo
