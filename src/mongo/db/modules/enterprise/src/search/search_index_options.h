/**
 * Copyright (C) 2023-present MongoDB, Inc.
 */

#pragma once

#include "mongo/base/status.h"
#include "mongo/db/tenant_id.h"

namespace mongo {

/**
 * Search index configuration options
 */
struct SearchIndexParams {
    static Status onValidateHost(StringData str, const boost::optional<TenantId>&);
    std::string host = "";
};

extern SearchIndexParams globalSearchIndexParams;

}  // namespace mongo
