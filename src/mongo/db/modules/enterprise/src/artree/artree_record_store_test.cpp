// artree_record_store_test.cpp

/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
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

#include <iostream>
#include <boost/shared_ptr.hpp>

#include "mongo/db/operation_context_noop.h"
#include "mongo/db/record_id.h"
#include "mongo/db/storage/record_store_test_harness.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"

#include "artree.h"
#include "artree_record_store.h"
#include "artree_recovery_unit.h"

namespace mongo {

class ARTreeRSHarnessHelper : public HarnessHelper {
public:
    ARTreeRSHarnessHelper() {}

    virtual RecoveryUnit* newRecoveryUnit() override {
        return new ARTreeRecoveryUnit(_engine);
    }

    virtual std::unique_ptr<RecordStore> newNonCappedRecordStore() override {
        StringData ns("artree.record_store.ns");
        StringData ident("artree.record_store.ident");
        auto art = ARTree::create(_engine);
        return std::unique_ptr<RecordStore>(new ARTreeRecordStore(ns, ident, art));
    }

    virtual std::unique_ptr<RecordStore> newCappedRecordStore(
        int64_t cappedMaxSize = kDefaultCapedSizeBytes, int64_t cappedMaxDocs = -1) override {
        StringData ns("artree.record_store.ns");
        StringData ident("artree.record_store.ident");
        auto art = ARTree::create(_engine, cappedMaxSize, cappedMaxDocs);
        return stdx::make_unique<ARTreeRecordStore>(ns, ident, art);
    }

    virtual bool supportsDocLocking() override {
        return true;
    }

    ARTreeKVEngine* getEngine() {
        return _engine;
    }

private:
    ARTreeKVEngine _engine[1];
};

std::unique_ptr<HarnessHelper> newHarnessHelper() {
    return stdx::make_unique<ARTreeRSHarnessHelper>();
}

TEST(ArtTreeRecordStore, BasicInsertTest) {
    std::unique_ptr<ARTreeRSHarnessHelper> harness = stdx::make_unique<ARTreeRSHarnessHelper>();
    ARTreeKVEngine* engine = harness->getEngine();
    std::unique_ptr<ARTreeRecoveryUnit> recUnit = stdx::make_unique<ARTreeRecoveryUnit>(engine);
    OperationContext* opCtx = new OperationContextNoop(recUnit.get());
    auto recStore = harness->newNonCappedRecordStore();

    StatusWith<RecordId> status =
        recStore->insertRecord(opCtx, "this is the record data", 23, false);
    ASSERT_OK(status.getStatus());
}

TEST(ArtTreeRecordStore, BasicReadAfterWriteTest) {
    std::unique_ptr<ARTreeRSHarnessHelper> harness = stdx::make_unique<ARTreeRSHarnessHelper>();
    ARTreeKVEngine* engine = harness->getEngine();
    std::unique_ptr<ARTreeRecoveryUnit> recUnit = stdx::make_unique<ARTreeRecoveryUnit>(engine);
    OperationContext* opCtx = new OperationContextNoop(recUnit.get());
    auto recStore = harness->newNonCappedRecordStore();

    StatusWith<RecordId> status1 =
        recStore->insertRecord(opCtx, "this is the first record data", 29, false);
    ASSERT_OK(status1.getStatus());

    StatusWith<RecordId> status2 =
        recStore->insertRecord(opCtx, "this is the second record data", 30, false);
    ASSERT_OK(status2.getStatus());

    StatusWith<RecordId> status3 =
        recStore->insertRecord(opCtx, "this is the third record data", 29, false);
    ASSERT_OK(status3.getStatus());

    char buf[1024];
    RecordData rd(buf, 1024);
    ASSERT_TRUE(recStore->findRecord(opCtx, status2.getValue(), &rd));
}
}
