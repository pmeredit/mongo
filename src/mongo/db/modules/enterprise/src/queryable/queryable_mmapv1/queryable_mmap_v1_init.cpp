/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/mmap_v1/mmap_v1_engine.h"
#include "mongo/db/storage/storage_engine_metadata.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/stdx/memory.h"

#include "../blockstore/context.h"
#include "queryable_global_options.h"
#include "queryable_mmap_v1_extent_manager.h"

namespace mongo {

namespace {

class QueryableMMAPV1Factory final : public StorageEngine::Factory {
public:
    ~QueryableMMAPV1Factory() override {}
    StorageEngine* create(const StorageGlobalParams& params,
                          const StorageEngineLockFile* lockFile) const override {
        uassert(ErrorCodes::InvalidOptions,
                "Queryable restores must be started with --queryableBackupMode",
                params.readOnly);

        uassert(ErrorCodes::InvalidOptions,
                "Cannot start queryable_mmapv1 without setting --queryableBackuApiUri and "
                "--queryableSnapshotId",
                queryable::queryableGlobalOptions.getApiUri() &&
                    queryable::queryableGlobalOptions.getSnapshotId());

        auto apiUri = *queryable::queryableGlobalOptions.getApiUri();
        auto snapshotId = *queryable::queryableGlobalOptions.getSnapshotId();

        auto context = queryable::Context(apiUri, snapshotId);

        return new MMAPV1Engine(
            lockFile,
            getGlobalServiceContext()->getFastClockSource(),
            stdx::make_unique<queryable::BlockstoreBackedExtentManager::Factory>(
                std::move(context)));
    }

    StringData getCanonicalName() const override {
        return "queryable_mmapv1"_sd;
    }

    Status validateMetadata(const StorageEngineMetadata& metadata,
                            const StorageGlobalParams& params) const override {
        return metadata.validateStorageEngineOption("directoryPerDB", params.directoryperdb);
    }

    BSONObj createMetadataOptions(const StorageGlobalParams& params) const override {
        BSONObjBuilder builder;
        builder.appendBool("directoryPerDB", params.directoryperdb);
        return builder.obj();
    }

    bool supportsReadOnly() const override {
        return true;
    };
};

}  // namespace

MONGO_INITIALIZER_WITH_PREREQUISITES(QueryableMMAPV1EngineInit, ("SetGlobalEnvironment"))
(InitializerContext* context) {
    getGlobalServiceContext()->registerStorageEngine("queryable_mmapv1",
                                                     new QueryableMMAPV1Factory());
    return Status::OK();
}

}  // namespace mongo
