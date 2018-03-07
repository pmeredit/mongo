/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>

#include "mongo/base/init.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_d.h"
#include "mongo/db/storage/kv/kv_storage_engine.h"
#include "mongo/db/storage/storage_engine_metadata.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_extensions.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_global_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_index.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_kv_engine.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_parameters.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_record_store.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_server_status.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_util.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"

#include "../blockstore/http_client.h"
#include "../queryable_mmapv1/queryable_global_options.h"
#include "../queryable_mmapv1/queryable_mmap_v1_extent_manager.h"

namespace mongo {

namespace {

class QueryableWtFactory final : public StorageEngine::Factory {
public:
    ~QueryableWtFactory() override {}
    StorageEngine* create(const StorageGlobalParams& params,
                          const StorageEngineLockFile* lockFile) const override {
        uassert(ErrorCodes::InvalidOptions,
                "Queryable restores must be started with --queryableBackupMode",
                params.readOnly);

        uassert(ErrorCodes::InvalidOptions,
                "Cannot start queryable_wt without setting --queryableBackuApiUri and "
                "--queryableSnapshotId",
                queryable::queryableGlobalOptions.getApiUri() &&
                    queryable::queryableGlobalOptions.getSnapshotId());

        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "Cannot start queryable_wt without setting the "
                              << queryable::kSecretKeyEnvVar
                              << " environment variable",
                std::getenv(queryable::kSecretKeyEnvVar) != nullptr);

        auto apiUri = *queryable::queryableGlobalOptions.getApiUri();
        auto snapshotId = *queryable::queryableGlobalOptions.getSnapshotId();

        size_t cacheMB = WiredTigerUtil::getCacheSizeMB(wiredTigerGlobalOptions.cacheSizeGB);
        const bool kEphemeral = false;

        uassert(ErrorCodes::InvalidOptions,
                "--queryableBackupApiUri cannot contain quotes",
                apiUri.find('"') == std::string::npos);
        uassert(ErrorCodes::InvalidOptions,
                "--dbpath cannot contain quotes",
                params.dbpath.find('"') == std::string::npos);

        // WiredTiger passes a non-normalized path to the operating system, e.g: a `dbpath` of
        // "/data/db/" will result in WiredTiger trying to open "/data/db//WiredTiger.turtle".
        // Because the underlying "filesystem" in `queryable_wt` does not go through the operating
        // system, we'll normalize the `dbpath` here to remove trailing slashes. This should
        // result in WiredTiger never making a callback with double slashes.
        std::string dbpath =
            (boost::filesystem::path(params.dbpath) / "dummy_filename").parent_path().string();

        std::string fsOptions = str::stream()
            << "local={entry=queryableWtFsCreate,early_load=true,config={apiUri=\"" << apiUri
            << "\",snapshotId=\"" << snapshotId << "\",dbpath=\"" << dbpath << "\"}}";

        WiredTigerExtensions::get(getGlobalServiceContext())->addExtension(fsOptions);

        WiredTigerKVEngine* kv =
            new WiredTigerKVEngine(getCanonicalName().toString(),
                                   dbpath,
                                   getGlobalServiceContext()->getFastClockSource(),
                                   wiredTigerGlobalOptions.engineConfig,
                                   cacheMB,
                                   params.dur,
                                   kEphemeral,
                                   params.repair,
                                   params.readOnly);
        kv->setRecordStoreExtraOptions(wiredTigerGlobalOptions.collectionConfig);
        kv->setSortedDataInterfaceExtraOptions(wiredTigerGlobalOptions.indexConfig);
        // Intentionally leaked.
        new WiredTigerServerStatusSection(kv);
        new WiredTigerEngineRuntimeConfigParameter(kv);

        KVStorageEngineOptions options;
        options.directoryPerDB = params.directoryperdb;
        options.directoryForIndexes = wiredTigerGlobalOptions.directoryForIndexes;
        options.forRepair = params.repair;
        return new KVStorageEngine(kv, options);
    }

    StringData getCanonicalName() const override {
        return "queryable_wt";
    }

    Status validateMetadata(const StorageEngineMetadata& metadata,
                            const StorageGlobalParams& params) const override {
        Status status =
            metadata.validateStorageEngineOption("directoryPerDB", params.directoryperdb);
        if (!status.isOK()) {
            return status;
        }

        status = metadata.validateStorageEngineOption("directoryForIndexes",
                                                      wiredTigerGlobalOptions.directoryForIndexes);
        if (!status.isOK()) {
            return status;
        }

        const bool kDefaultGroupCollections = false;
        status =
            metadata.validateStorageEngineOption("groupCollections",
                                                 params.groupCollections,
                                                 boost::optional<bool>(kDefaultGroupCollections));
        if (!status.isOK()) {
            return status;
        }

        return Status::OK();
    }

    BSONObj createMetadataOptions(const StorageGlobalParams& params) const override {
        BSONObjBuilder builder;
        builder.appendBool("directoryPerDB", params.directoryperdb);
        return builder.obj();
    }

    bool supportsReadOnly() const override {
        return true;
    }
};

}  // namespace

MONGO_INITIALIZER(QueryableWtEngineInit)
(InitializerContext* context) {
    getGlobalServiceContext()->registerStorageEngine("queryable_wt", new QueryableWtFactory());
    return Status::OK();
}

}  // namespace mongo
