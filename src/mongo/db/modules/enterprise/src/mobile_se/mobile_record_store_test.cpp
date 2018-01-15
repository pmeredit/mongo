/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mobile_record_store.h"

#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>

#include "mobile_recovery_unit.h"
#include "mobile_session.h"
#include "mobile_session_pool.h"
#include "mobile_sqlite_statement.h"
#include "mobile_util.h"
#include "mongo/base/init.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/storage/record_store_test_harness.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"


namespace mongo {

namespace {

static int inc = 0;

class MobileHarnessHelper final : public RecordStoreHarnessHelper {
public:
    MobileHarnessHelper() : _dbPath("mobile_record_store_harness") {
        // TODO: Determine if this should be util function.
        boost::system::error_code err;
        boost::filesystem::path dir(_dbPath.path());

        if (!boost::filesystem::exists(dir, err)) {
            if (err) {
                uasserted(ErrorCodes::UnknownError, err.message());
            }

            boost::filesystem::create_directory(dir, err);
            if (err) {
                uasserted(ErrorCodes::UnknownError, err.message());
            }
        }

        boost::filesystem::path file("mobile.sqlite");
        boost::filesystem::path fullPath = dir / file;

        if (boost::filesystem::exists(fullPath, err)) {
            if (err) {
                uasserted(ErrorCodes::UnknownError, err.message());
            } else if (!boost::filesystem::is_regular_file(fullPath)) {
                std::string errMsg("Failed to open " + dir.generic_string() +
                                   ": not a regular file");
                uasserted(ErrorCodes::BadValue, errMsg);
            }
        }

        _fullPath = fullPath.string();
        _sessionPool.reset(new MobileSessionPool(_fullPath));
    }

    std::unique_ptr<RecordStore> newNonCappedRecordStore() override {
        inc++;
        return newNonCappedRecordStore("table_" + std::to_string(inc));
    }

    std::unique_ptr<RecordStore> newNonCappedRecordStore(const std::string& ns) override {
        ServiceContext::UniqueOperationContext opCtx(this->newOperationContext());
        MobileRecordStore::create(opCtx.get(), ns);
        return stdx::make_unique<MobileRecordStore>(
            opCtx.get(), ns, _fullPath, ns, CollectionOptions());
    }

    std::unique_ptr<RecordStore> newCappedRecordStore(int64_t cappedMaxSize,
                                                      int64_t cappedMaxDocs) override {
        inc++;
        return newCappedRecordStore("table_" + std::to_string(inc), cappedMaxSize, cappedMaxDocs);
    }

    std::unique_ptr<RecordStore> newCappedRecordStore(const std::string& ns,
                                                      int64_t cappedMaxSize,
                                                      int64_t cappedMaxDocs) override {
        ServiceContext::UniqueOperationContext opCtx(this->newOperationContext());
        MobileRecordStore::create(opCtx.get(), ns);
        CollectionOptions options;
        options.capped = true;
        options.cappedSize = cappedMaxSize;
        options.cappedMaxDocs = cappedMaxDocs;
        return stdx::make_unique<MobileRecordStore>(opCtx.get(), ns, _fullPath, ns, options);
    }

    std::unique_ptr<RecoveryUnit> newRecoveryUnit() final {
        return stdx::make_unique<MobileRecoveryUnit>(_sessionPool.get());
    }

    bool supportsDocLocking() final {
        return false;
    }

private:
    unittest::TempDir _dbPath;
    std::string _fullPath;
    std::unique_ptr<MobileSessionPool> _sessionPool;
};

std::unique_ptr<HarnessHelper> makeHarnessHelper() {
    return stdx::make_unique<MobileHarnessHelper>();
}

MONGO_INITIALIZER(RegisterHarnessFactory)(InitializerContext* const) {
    mongo::registerHarnessHelperFactory(makeHarnessHelper);
    return Status::OK();
}
}  // namespace
}  // namespace mongo
