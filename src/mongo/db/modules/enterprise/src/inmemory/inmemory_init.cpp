/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
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

#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>

#include "inmemory_global_options.h"
#include "inmemory_options_init.h"

#include "mongo/base/init.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_d.h"
#include "mongo/db/storage/kv/kv_storage_engine.h"
#include "mongo/db/storage/storage_engine_lock_file.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_kv_engine.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_parameters.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_server_status.h"

namespace mongo {

namespace {

class InMemoryFactory final : public StorageEngine::Factory {
public:
    virtual ~InMemoryFactory() {}

    StorageEngine* create(const StorageGlobalParams& params,
                          const StorageEngineLockFile* lockFile) const final {
        boost::filesystem::path dbpath = params.dbpath;
        dbpath /= "/inmem";
        boost::filesystem::remove_all(dbpath);
        boost::filesystem::create_directory(dbpath);

        std::string engineConfig = inMemoryGlobalOptions.engineConfig;
        engineConfig +=
            "in_memory=true,log=(enabled=false),"
            "file_manager=(close_idle_time=0),checkpoint=(wait=0,log_size=0)";

        size_t cacheMB = WiredTigerUtil::getCacheSizeMB(inMemoryGlobalOptions.inMemorySizeGB);
        const bool durable = false;
        const bool ephemeral = true;
        const bool repair = false;
        const bool readOnly = false;
        WiredTigerKVEngine* kv =
            new WiredTigerKVEngine(getCanonicalName().toString(),
                                   dbpath.string(),
                                   getGlobalServiceContext()->getFastClockSource(),
                                   engineConfig,
                                   cacheMB,
                                   durable,
                                   ephemeral,
                                   repair,
                                   readOnly);
        kv->setRecordStoreExtraOptions(inMemoryGlobalOptions.collectionConfig);
        kv->setSortedDataInterfaceExtraOptions(inMemoryGlobalOptions.indexConfig);
        // Intentionally leaked.
        new WiredTigerServerStatusSection(kv);
        new WiredTigerEngineRuntimeConfigParameter(kv);

        KVStorageEngineOptions options;
        options.directoryPerDB = false;
        options.directoryForIndexes = false;
        options.forRepair = false;

        return new KVStorageEngine(kv, options);
    }

    virtual StringData getCanonicalName() const final {
        return "inMemory";
    }

    virtual Status validateCollectionStorageOptions(const BSONObj& options) const final {
        return Status::OK();
    }

    virtual Status validateIndexStorageOptions(const BSONObj& options) const final {
        return Status::OK();
    }

    virtual Status validateMetadata(const StorageEngineMetadata& metadata,
                                    const StorageGlobalParams& params) const final {
        return Status::OK();
    }

    virtual BSONObj createMetadataOptions(const StorageGlobalParams& params) const final {
        return BSONObj();
    }
};

}  // namespace

// XXX: The customization hook mechanism only supports a single customizer. That is enough
// for now, since the two enterprise modules that configure customization hooks (encryption
// and in-memory) are mutually exclusive.
MONGO_INITIALIZER_WITH_PREREQUISITES(InMemoryEngineInit,
                                     ("SetGlobalEnvironment", "SetWiredTigerCustomizationHooks"))
(InitializerContext* context) {
    getGlobalServiceContext()->registerStorageEngine("inMemory", new InMemoryFactory());
    if (storageGlobalParams.engine == "inMemory") {
        auto optionManager = stdx::make_unique<InMemoryConfigManager>(storageGlobalParams.dbpath);
        WiredTigerCustomizationHooks::set(getGlobalServiceContext(), std::move(optionManager));
    }
    return Status::OK();
}

}  // namespace mongo
