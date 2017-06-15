/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>

#include "mongo/base/init.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/sorted_data_interface_test_harness.h"
#include "mongo/platform/basic.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "moose_index.h"
#include "moose_recovery_unit.h"
#include "moose_session_pool.h"

namespace mongo {
int inc = 0;

class MooseIndexTestHarnessHelper final : public virtual SortedDataInterfaceHarnessHelper {
public:
    MooseIndexTestHarnessHelper()
        : _dbPath("moose_index_harness"), _ordering(Ordering::make(BSONObj())) {
        boost::filesystem::path fullPath(_dbPath.path());
        fullPath /= "moose.sqlite";
        _fullPath = fullPath.string();
        _sessionPool.reset(new MooseSessionPool(_fullPath));
    }

    std::unique_ptr<SortedDataInterface> newSortedDataInterface(bool isUnique) {
        std::string ident("index_" + std::to_string(inc++));
        OperationContextNoop opCtx(newRecoveryUnit().release());
        Status status = MooseIndex::create(&opCtx, ident);
        fassertStatusOK(37052, status);

        if (isUnique) {
            return stdx::make_unique<MooseIndexUnique>(_ordering, ident);
        }
        return stdx::make_unique<MooseIndexStandard>(_ordering, ident);
    }

    std::unique_ptr<RecoveryUnit> newRecoveryUnit() {
        return stdx::make_unique<MooseRecoveryUnit>(_sessionPool.get());
    }

private:
    unittest::TempDir _dbPath;
    std::string _fullPath;
    std::unique_ptr<MooseSessionPool> _sessionPool;
    const Ordering _ordering;
};

std::unique_ptr<HarnessHelper> makeHarnessHelper() {
    return stdx::make_unique<MooseIndexTestHarnessHelper>();
}

MONGO_INITIALIZER(RegisterHarnessFactory)(InitializerContext* const) {
    mongo::registerHarnessHelperFactory(makeHarnessHelper);
    return Status::OK();
}
}  // namespace mongo
