/**
 * Copyright (C) 2014-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include <iostream>

#include "inmemory_options_init.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage


//#include "mongo/db/storage/data_protector.h"

namespace mongo {

InMemoryConfigManager::InMemoryConfigManager(const std::string& dbPath) : _dbPath(dbPath) {}

InMemoryConfigManager::~InMemoryConfigManager() {}

// Add a special configuration option for MongoDB metadata tables, so the
// in-memory storage engine doesn't need to handle WT_CACHE_FULL error returns
// from those tables.
std::string InMemoryConfigManager::getTableCreateConfig(StringData tableName) {
    std::string config;

    // Internal metadata WT tables such as sizeStorer and _mdb_catalog are identified by not
    // having a '.' separated name that distinguishes a "normal namespace during collection
    // or index creation.
    std::size_t dotIndex = tableName.find(".");
    if (dotIndex == std::string::npos && tableName != StringData("system")) {
        LOGV2_DEBUG(24013,
                    2,
                    "Adding custom table create config for: {tableName}",
                    "tableName"_attr = tableName);
        config += "ignore_in_memory_cache_size=true,";
    }

    return config;
}

bool InMemoryConfigManager::enabled() const {
    return true;
}
}  // namespace mongo
