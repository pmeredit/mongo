/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#include "mongo/base/init.h"
#include "mongo/db/storage/kv/kv_engine_test_harness.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/temp_dir.h"

#include "moose_kv_engine.h"

namespace mongo {
namespace {

class MooseKVHarnessHelper : public KVHarnessHelper {
public:
    MooseKVHarnessHelper() : _dbPath("moose_kv_engine_harness") {
        _engine = stdx::make_unique<MooseKVEngine>(_dbPath.path());
    }

    virtual KVEngine* restartEngine() {
        _engine.reset(new MooseKVEngine(_dbPath.path()));
        return _engine.get();
    }

    virtual KVEngine* getEngine() {
        return _engine.get();
    }

private:
    std::unique_ptr<MooseKVEngine> _engine;
    unittest::TempDir _dbPath;
};

std::unique_ptr<KVHarnessHelper> makeHelper() {
    return stdx::make_unique<MooseKVHarnessHelper>();
}

MONGO_INITIALIZER(RegisterKVHarnessFactory)(InitializerContext*) {
    KVHarnessHelper::registerFactory(makeHelper);
    return Status::OK();
}

}  // namespace
}  // namespace mongo
