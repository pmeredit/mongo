/**
 * Copyright (C) 2014 MongoDB, Inc.  All Rights Reserved.
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
