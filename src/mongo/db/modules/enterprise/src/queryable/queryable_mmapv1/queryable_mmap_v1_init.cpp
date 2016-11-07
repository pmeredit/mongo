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
#include "mongo/util/processinfo.h"

#include "../blockstore/context.h"
#include "../blockstore/http_client.h"
#include "queryable_global_options.h"
#include "queryable_mmap_v1_extent_manager.h"

namespace mongo {

namespace {

/**
 * An input of > 0 implies a user-specified input. An input of zero will calculate a reasonable
 * quota.
 */
std::size_t getMemoryQuotaMB(boost::optional<double> requestedMemoryQuotaMB) {
    if (requestedMemoryQuotaMB) {
        return static_cast<size_t>(*requestedMemoryQuotaMB);
    }

    const double kDefaultMemoryQuotaMB = 64.0;
    // Choose a reasonable amount of cache when not explicitly specified by user.  Set a minimum of
    // 64MB, otherwise use 20% of available memory over 1GB.
    ProcessInfo processInfo;
    const double memSizeMB = processInfo.getMemSizeMB();
    double ret = std::max((memSizeMB - 1024) * 0.2, kDefaultMemoryQuotaMB);
    return static_cast<std::size_t>(ret);
}

class QueryableMMAPV1Factory final : public StorageEngine::Factory {
public:
    ~QueryableMMAPV1Factory() override {}
    StorageEngine* create(const StorageGlobalParams& params,
                          const StorageEngineLockFile* lockFile) const override {
        uassert(ErrorCodes::InvalidOptions,
                "Queryable restores must be started with --queryableBackupMode",
                params.readOnly);

        uassert(ErrorCodes::InvalidOptions,
                "Cannot start queryable_mmapv1 without setting --queryableBackupApiUri and "
                "--queryableSnapshotId",
                queryable::queryableGlobalOptions.getApiUri() &&
                    queryable::queryableGlobalOptions.getSnapshotId());

        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "Cannot start queryable_mmapv1 without setting the "
                              << queryable::kSecretKeyEnvVar
                              << " environment variable",
                std::getenv(queryable::kSecretKeyEnvVar) != nullptr);

        auto apiUri = *queryable::queryableGlobalOptions.getApiUri();
        auto snapshotId = *queryable::queryableGlobalOptions.getSnapshotId();

        std::size_t memoryQuotaMB =
            getMemoryQuotaMB(queryable::queryableGlobalOptions.getMemoryQuotaMB());

        auto context = queryable::Context(apiUri, snapshotId);

        return new MMAPV1Engine(
            lockFile,
            getGlobalServiceContext()->getFastClockSource(),
            stdx::make_unique<queryable::BlockstoreBackedExtentManager::Factory>(
                std::move(context), memoryQuotaMB * 1024 * 1024));
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
