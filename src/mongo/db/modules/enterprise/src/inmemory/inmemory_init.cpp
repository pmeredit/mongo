/**
 * Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>

#include "inmemory_global_options.h"
#include "inmemory_options_init.h"

#include "mongo/base/init.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/storage_engine_impl.h"
#include "mongo/db/storage/storage_engine_init.h"
#include "mongo/db/storage/storage_engine_lock_file.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_index.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_kv_engine.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_record_store.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_server_status.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_util.h"

namespace mongo {

namespace {

class InMemoryFactory final : public StorageEngine::Factory {
public:
    ~InMemoryFactory() override {}

    std::unique_ptr<StorageEngine> create(OperationContext* opCtx,
                                          const StorageGlobalParams& params,
                                          const StorageEngineLockFile* lockFile) const final {
        boost::filesystem::path dbpath = params.dbpath;
        dbpath /= "/inmem";
        boost::filesystem::remove_all(dbpath);
        boost::filesystem::create_directory(dbpath);


        WiredTigerKVEngine::WiredTigerConfig wtConfig = getWiredTigerConfigFromStartupOptions();
        wtConfig.cacheSizeMB = WiredTigerUtil::getCacheSizeMB(inMemoryGlobalOptions.inMemorySizeGB);
        wtConfig.extraOpenOptions = inMemoryGlobalOptions.engineConfig;
        wtConfig.extraOpenOptions +=
            ",file_manager=(close_idle_time=0),checkpoint=(wait=0,log_size=0)";
        wtConfig.inMemory = true;
        wtConfig.logEnabled = false;
        const bool ephemeral = true;
        const bool repair = false;
        auto kv =
            std::make_unique<WiredTigerKVEngine>(getCanonicalName().toString(),
                                                 dbpath.string(),
                                                 getGlobalServiceContext()->getFastClockSource(),
                                                 std::move(wtConfig),
                                                 ephemeral,
                                                 repair);
        kv->setRecordStoreExtraOptions(inMemoryGlobalOptions.collectionConfig);
        kv->setSortedDataInterfaceExtraOptions(inMemoryGlobalOptions.indexConfig);

        // We're using a WT-based engine; register the ServerStatusSection for it.
        *ServerStatusSectionBuilder<WiredTigerServerStatusSection>(
             std::string{WiredTigerServerStatusSection::kServerStatusSectionName})
             .forShard();

        StorageEngineOptions options;
        options.directoryPerDB = false;
        options.directoryForIndexes = false;
        options.forRepair = false;
        options.forRestore = false;

        return std::make_unique<StorageEngineImpl>(opCtx, std::move(kv), options);
    }

    StringData getCanonicalName() const final {
        return "inMemory";
    }

    Status validateCollectionStorageOptions(const BSONObj& options) const final {
        return WiredTigerRecordStore::parseOptionsField(options).getStatus();
    }

    Status validateIndexStorageOptions(const BSONObj& options) const final {
        return WiredTigerIndex::parseIndexOptions(options).getStatus();
    }

    Status validateMetadata(const StorageEngineMetadata& metadata,
                            const StorageGlobalParams& params) const final {
        return Status::OK();
    }

    BSONObj createMetadataOptions(const StorageGlobalParams& params) const final {
        return BSONObj();
    }
};

ServiceContext::ConstructorActionRegisterer registerInMemoryEngineInit{
    "InMemoryEngineInit", {"SetWiredTigerCustomizationHooks"}, [](ServiceContext* service) {
        registerStorageEngine(service, std::make_unique<InMemoryFactory>());
        if (storageGlobalParams.engine == "inMemory") {
            auto optionManager =
                std::make_unique<InMemoryConfigManager>(storageGlobalParams.dbpath);
            WiredTigerCustomizationHooks::set(service, std::move(optionManager));
        }
    }};
}  // namespace
}  // namespace mongo
