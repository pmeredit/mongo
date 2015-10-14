// artree_sorted_data_impl_test.cpp

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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include <fstream>
#include <iostream>
#include <string>
#include <time.h>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/json.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/record_id.h"
#include "mongo/db/storage/sorted_data_interface_test_harness.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/log.h"

#include "artree.h"
#include "artree_debug.h"
#include "artree_kv_engine.h"
#include "artree_record_store.h"
#include "artree_recovery_unit.h"
#include "artree_sorted_data_impl.h"

namespace mongo {

class ARTreeSDHarnessHelper : public HarnessHelper {
public:
    ARTreeSDHarnessHelper() : _art(nullptr) {
        if (FUNCTION_TRACE)
            log() << __FILE__ << " : " << __FUNCTION__;
    }

    virtual std::unique_ptr<RecoveryUnit> newRecoveryUnit() {
        if (FUNCTION_TRACE)
            log() << __FILE__ << " : " << __FUNCTION__;
        return stdx::make_unique<ARTreeRecoveryUnit>(_engine);
    }

    virtual std::unique_ptr<SortedDataInterface> newSortedDataInterface(bool unique) {
        if (FUNCTION_TRACE)
            log() << __FILE__ << " : " << __FUNCTION__;

        BSONObj spec = BSON("key" << BSON("a" << 1) << "name"
                                  << "testIndex"
                                  << "ns"
                                  << "test.artree");

        IndexDescriptor desc(nullptr, "", spec);
        Ordering ordering = Ordering::make(desc.keyPattern());
        if (!_art) {
            StringData ident("artree.sorted_data_impl.ident");
            _art = ARTree::create(_engine);
        }
        ARTreeIndex* index = new ARTreeIndex(_art);
        return stdx::make_unique<ARTreeSortedDataImpl>(ordering, index);
    }

    std::unique_ptr<SortedDataInterface> newSortedDataInterface(
        bool unique, std::initializer_list<IndexKeyEntry> toInsert) {
        if (FUNCTION_TRACE)
            log() << __FILE__ << " : " << __FUNCTION__;
        return newSortedDataInterface(unique);
    }

    ARTreeKVEngine* getEngine() {
        return _engine;
    }

private:
    ARTree* _art;
    ARTreeKVEngine _engine[1];
};

std::unique_ptr<HarnessHelper> newHarnessHelper() {
    if (FUNCTION_TRACE)
        log() << __FILE__ << " : " << __FUNCTION__;
    return stdx::make_unique<ARTreeSDHarnessHelper>();
}

TEST(ArtTreeSortedDataImpl, BasicInsertTest) {
    ARTreeSDHarnessHelper* harnessHelper = new ARTreeSDHarnessHelper();
    StringData ns("artree.record_store.ns");
    StringData ident("artree.record_store.ident");

    ARTreeKVEngine* engine = harnessHelper->getEngine();
    ARTree* art = ARTree::create(engine);
    ARTreeRecoveryUnit* recoveryUnit = new ARTreeRecoveryUnit(engine);
    OperationContext* opCtx = new OperationContextNoop(recoveryUnit);
    ARTreeRecordStore recStore(ns, ident, art);

    // insert records
    BSONObj o1 = BSON("_id" << 1 << "a" << 10);
    StatusWith<RecordId> status = recStore.insertRecord(opCtx, o1.objdata(), o1.objsize(), false);
    ASSERT_OK(status.getStatus());
    RecordId loc1 = status.getValue();

    BSONObj o2 = BSON("_id" << 2 << "a" << 20);
    status = recStore.insertRecord(opCtx, o2.objdata(), o2.objsize(), false);
    ASSERT_OK(status.getStatus());
    RecordId loc2 = status.getValue();

    BSONObj o3 = BSON("_id" << 3 << "a" << 30);
    status = recStore.insertRecord(opCtx, o3.objdata(), o3.objsize(), false);
    ASSERT_OK(status.getStatus());
    RecordId loc3 = status.getValue();

    BSONObj o4 = BSON("_id" << 4 << "a" << 40);
    status = recStore.insertRecord(opCtx, o4.objdata(), o4.objsize(), false);
    ASSERT_OK(status.getStatus());
    RecordId loc4 = status.getValue();

    // create index and SortedDataInterface to this index
    BSONObj spec = BSON("key" << BSON("a" << 1) << "name"
                              << "testIndex"
                              << "ns"
                              << "test.art");
    IndexDescriptor desc(nullptr, "", spec);
    Ordering ordering = Ordering::make(desc.keyPattern());

    ARTreeIndex* index = new ARTreeIndex(art);
    ARTreeSortedDataImpl* indexInterface = new ARTreeSortedDataImpl(ordering, index);

    // insert keys
    ASSERT_OK(indexInterface->insert(opCtx, BSON("" << 10), loc1, false /*not unique*/));
    ASSERT_OK(indexInterface->insert(opCtx, BSON("" << 20), loc2, false /*not unique*/));
    ASSERT_OK(indexInterface->insert(opCtx, BSON("" << 30), loc3, false /*not unique*/));
    ASSERT_OK(indexInterface->insert(opCtx, BSON("" << 40), loc4, false /*not unique*/));

    // cursor, allocate and position
    std::unique_ptr<SortedDataInterface::Cursor> cursor = indexInterface->newCursor(opCtx);
    BSONObj startKey = BSON("" << 20);
    BSONObj endKey = BSON("l" << 40);

    cursor->setEndPosition(endKey, false /*exclusive*/);
    boost::optional<IndexKeyEntry> e = cursor->seek(startKey, true /*inclusive*/);

    // cursor, traverse
    for (; boost::none != e; e = cursor->next()) {
        std::cout << "key = " << e.get().key.toString(0, 0) << std::endl;
        std::cout << "loc = " << e.get().loc.repr() << std::endl;
    }
}
}
