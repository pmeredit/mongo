/**
 * Copyright (C) 2014 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "inmemory_global_options.h"

#include "mongo/base/status.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

InMemoryGlobalOptions inMemoryGlobalOptions;

namespace {
MONGO_STARTUP_OPTIONS_STORE(InMemoryOptions)(InitializerContext*) {
    const auto& params = optionenvironment::startupOptionsParsed;

    // InMemory storage engine options
    if (params.count("storage.inMemory.engineConfig.inMemorySizeGB")) {
        inMemoryGlobalOptions.inMemorySizeGB =
            params["storage.inMemory.engineConfig.inMemorySizeGB"].as<double>();
    }
    if (params.count("storage.inMemory.engineConfig.statisticsLogDelaySecs")) {
        inMemoryGlobalOptions.statisticsLogDelaySecs =
            params["storage.inMemory.engineConfig.statisticsLogDelaySecs"].as<int>();
    }
    if (params.count("storage.inMemory.engineConfig.configString")) {
        inMemoryGlobalOptions.engineConfig =
            params["storage.inMemory.engineConfig.configString"].as<std::string>();
        log() << "Engine custom option: " << inMemoryGlobalOptions.engineConfig;
    }

    // InMemory collection options
    if (params.count("storage.inMemory.collectionConfig.configString")) {
        inMemoryGlobalOptions.collectionConfig =
            params["storage.inMemory.collectionConfig.configString"].as<std::string>();
        log() << "Collection custom option: " << inMemoryGlobalOptions.collectionConfig;
    }

    // InMemory index options
    if (params.count("storage.inMemory.indexConfig.configString")) {
        inMemoryGlobalOptions.indexConfig =
            params["storage.inMemory.indexConfig.configString"].as<std::string>();
        log() << "Index custom option: " << inMemoryGlobalOptions.indexConfig;
    }

    return Status::OK();
}
}  // namespace

}  // namespace mongo
