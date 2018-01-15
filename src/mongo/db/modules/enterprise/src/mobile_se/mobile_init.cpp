/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "../third_party/sqlite/sqlite3.h"
#include "mobile_kv_engine.h"
#include "mongo/base/init.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/kv/kv_storage_engine.h"
#include "mongo/db/storage/storage_options.h"

namespace mongo {

namespace {
class MobileFactory : public StorageEngine::Factory {
public:
    StorageEngine* create(const StorageGlobalParams& params,
                          const StorageEngineLockFile* lockFile) const override {
        uassert(ErrorCodes::InvalidOptions,
                "mobile does not support --groupCollections",
                !params.groupCollections);

        KVStorageEngineOptions options;
        options.directoryPerDB = params.directoryperdb;
        options.forRepair = params.repair;

        MobileKVEngine* kvEngine = new MobileKVEngine(params.dbpath);
        return new KVStorageEngine(kvEngine, options);
    }

    StringData getCanonicalName() const override {
        return "mobile";
    }

    Status validateMetadata(const StorageEngineMetadata& metadata,
                            const StorageGlobalParams& params) const override {
        return Status::OK();
    }

    BSONObj createMetadataOptions(const StorageGlobalParams& params) const override {
        return BSONObj();
    }
};
}  // namespace

MONGO_INITIALIZER_WITH_PREREQUISITES(MobileKVEngineInit, ("SetGlobalEnvironment"))
(InitializerContext* context) {
    getGlobalServiceContext()->registerStorageEngine("mobile", new MobileFactory());
    return Status::OK();
}

}  // namespace mongo
