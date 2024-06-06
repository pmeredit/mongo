/**
 * Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/storage/wiredtiger/wiredtiger_customization_hooks.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

/**
 * The InMemoryConfigManager manages configuration options particular to the
 * in-memory storage engine.
 */
class InMemoryConfigManager : public WiredTigerCustomizationHooks {
    InMemoryConfigManager(const InMemoryConfigManager&) = delete;
    InMemoryConfigManager& operator=(const InMemoryConfigManager&) = delete;

public:
    /**
     * Initializes the InMemoryConfigManager.
     */
    InMemoryConfigManager(const std::string& dbPath);

    ~InMemoryConfigManager() override;

    bool enabled() const override;

    std::string getTableCreateConfig(StringData tableName) override;

private:
    const std::string _dbPath;
};
}  // namespace mongo
