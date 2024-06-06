/**
 * Copyright (C) 2014-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "inmemory_global_options.h"

#include "mongo/base/status.h"
#include "mongo/logv2/log.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage


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
        LOGV2(24190,
              "Engine custom option: {inMemoryGlobalOptions_engineConfig}",
              "inMemoryGlobalOptions_engineConfig"_attr = inMemoryGlobalOptions.engineConfig);
    }

    // InMemory collection options
    if (params.count("storage.inMemory.collectionConfig.configString")) {
        inMemoryGlobalOptions.collectionConfig =
            params["storage.inMemory.collectionConfig.configString"].as<std::string>();
        LOGV2(24191,
              "Collection custom option: {inMemoryGlobalOptions_collectionConfig}",
              "inMemoryGlobalOptions_collectionConfig"_attr =
                  inMemoryGlobalOptions.collectionConfig);
    }

    // InMemory index options
    if (params.count("storage.inMemory.indexConfig.configString")) {
        inMemoryGlobalOptions.indexConfig =
            params["storage.inMemory.indexConfig.configString"].as<std::string>();
        LOGV2(24192,
              "Index custom option: {inMemoryGlobalOptions_indexConfig}",
              "inMemoryGlobalOptions_indexConfig"_attr = inMemoryGlobalOptions.indexConfig);
    }
}
}  // namespace

}  // namespace mongo
