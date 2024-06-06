/**
 * Copyright (C) 2014-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <string>

namespace mongo {

struct InMemoryGlobalOptions {
    double inMemorySizeGB{0.0};
    std::size_t statisticsLogDelaySecs{0};

    std::string engineConfig;
    std::string collectionConfig;
    std::string indexConfig;
};

extern InMemoryGlobalOptions inMemoryGlobalOptions;

}  // namespace mongo
