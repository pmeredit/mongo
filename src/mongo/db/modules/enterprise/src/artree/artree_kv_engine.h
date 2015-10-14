// artree_kv_engine.h

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

#pragma once

#include "artree_ordered_list.h"
#include "mongo/bson/ordering.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/util/string_map.h"

namespace mongo {

class RecordStore;
class IndexDescriptor;
class SortedDataInterface;
class ARTreeRecoveryUnit;
class ARTreeIndex;
struct ARTree;

class ARTreeKVEngine : public KVEngine {
public:
    enum ReaderWriterEnum { en_reader, en_writer };

    ARTreeKVEngine();
    virtual ~ARTreeKVEngine() = default;

    virtual RecoveryUnit* newRecoveryUnit() override;

    virtual RecordStore* getRecordStore(OperationContext* opCtx,
                                        StringData ns,
                                        StringData ident,
                                        const CollectionOptions& options) override;

    virtual SortedDataInterface* getSortedDataInterface(OperationContext* opCtx,
                                                        StringData ident,
                                                        const IndexDescriptor* desc) override;

    virtual Status createRecordStore(OperationContext* opCtx,
                                     StringData ns,
                                     StringData ident,
                                     const CollectionOptions& options) override;

    virtual Status createSortedDataInterface(OperationContext* opCtx,
                                             StringData ident,
                                             const IndexDescriptor* desc) override;

    virtual bool supportsDocLocking() const override;
    virtual bool supportsDirectoryPerDB() const override;

    // sort of the main point
    virtual bool isDurable() const override {
        return false;
    }

    virtual int64_t getIdentSize(OperationContext* opCtx, StringData ident) override;
    virtual Status repairIdent(OperationContext* opCtx, StringData ident) override;
    virtual Status dropIdent(OperationContext* opCtx, StringData ident) override;
    virtual bool hasIdent(OperationContext* opCtx, StringData ident) const override;
    std::vector<std::string> getAllIdents(OperationContext* opCtx) const override;

    virtual int flushAllFiles(bool sync) override;

    virtual Status okToRename(OperationContext* opCtx,
                              StringData fromNS,
                              StringData toNS,
                              StringData ident,
                              const RecordStore* originalRecordStore) const override;

    virtual void cleanShutdown() override;

    uint64_t allocTxnId(ReaderWriterEnum e);

    uint64_t getMinimumTxnId() {
        return _timestamps->getMinimum();
    }

    void dequeueTimestamp(ARTreeOrderedList::Entry* entry) {
        if (entry->_value)
            _timestamps->remove(entry);
    }

    void enqueueTimestamp(ARTreeOrderedList::Entry* entry) {
        _timestamps->insert(entry, allocTxnId(en_reader));
    }

    bool isObsolete(uint64_t ts) {
        return ts >= _timestamps->getMinimum();
    }

private:
    Ordering _getOrdering(const IndexDescriptor* desc);
    ARTreeIndex* _getIndex(StringData ident, const IndexDescriptor* desc);
    ARTree* _getARTree(StringData ident);

    /**
     * ordered list of timestamps in recovery units
     * we use this to determine the minimum transaction id we can issue to a cursor.
     * if a frame transaction id is less than this minimum, the frame can be recycled.
     */
    ARTreeOrderedList _timestamps[1];

    /**
     * highest transaction id issued
     */
    volatile std::atomic_uint_fast64_t _globalTxnId;

    /**
     * map: ident -> index
     */
    StringMap<ARTreeIndex*> _indexMap;

    /**
     * map: ident -> artree (for the record store)
     */
    StringMap<ARTree*> _artMap;

    /**
     * mutex: protection for creating/deleting artrees
     */
    char _mutex[1];
};

}  // namespace mongo
