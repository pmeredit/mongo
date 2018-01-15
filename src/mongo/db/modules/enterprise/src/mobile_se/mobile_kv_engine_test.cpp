/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#include "mongo/base/init.h"
#include "mongo/db/storage/kv/kv_engine_test_harness.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/temp_dir.h"

#include "mobile_kv_engine.h"

namespace mongo {
namespace {

class MobileKVHarnessHelper : public KVHarnessHelper {
public:
    MobileKVHarnessHelper() : _dbPath("mobile_kv_engine_harness") {
        _engine = stdx::make_unique<MobileKVEngine>(_dbPath.path());
    }

    virtual KVEngine* restartEngine() {
        _engine.reset(new MobileKVEngine(_dbPath.path()));
        return _engine.get();
    }

    virtual KVEngine* getEngine() {
        return _engine.get();
    }

private:
    std::unique_ptr<MobileKVEngine> _engine;
    unittest::TempDir _dbPath;
};

std::unique_ptr<KVHarnessHelper> makeHelper() {
    return stdx::make_unique<MobileKVHarnessHelper>();
}

MONGO_INITIALIZER(RegisterKVHarnessFactory)(InitializerContext*) {
    KVHarnessHelper::registerFactory(makeHelper);
    return Status::OK();
}

}  // namespace
}  // namespace mongo
