/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage
#include "mongo/platform/basic.h"

#include "mongo/util/options_parser/startup_option_init.h"

#include <iostream>

#include "inmemory_global_options.h"
#include "inmemory_options_init.h"

#include "mongo/db/storage/data_protector.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

MONGO_MODULE_STARTUP_OPTIONS_REGISTER(InMemoryOptions)(InitializerContext* context) {
    return inMemoryGlobalOptions.add(&moe::startupOptions);
}

MONGO_STARTUP_OPTIONS_VALIDATE(InMemoryOptions)(InitializerContext* context) {
    return Status::OK();
}

MONGO_STARTUP_OPTIONS_STORE(InMemoryOptions)(InitializerContext* context) {
    Status ret = inMemoryGlobalOptions.store(moe::startupOptionsParsed, context->args());
    if (!ret.isOK()) {
        std::cerr << ret.toString() << std::endl;
        std::cerr << "try '" << context->args()[0] << " --help' for more information" << std::endl;
        ::_exit(EXIT_BADOPTIONS);
    }
    return Status::OK();
}

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
        LOG(2) << "Adding custom table create config for: " << tableName;
        config += "ignore_in_memory_cache_size=true,";
    }

    return config;
}

bool InMemoryConfigManager::enabled() const {
    return true;
}
}
