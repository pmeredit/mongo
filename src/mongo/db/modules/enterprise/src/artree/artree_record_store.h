// artree_record_store.h

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

#include "mongo/db/storage/capped_callback.h"
#include "mongo/db/storage/record_data.h"
#include "mongo/db/storage/record_store.h"

#include "artree_records.h"
#include "artree_iterator.h"

namespace mongo {

class ARTreeRecordCursor;
class ARTreeRecordReverseCursor;
class ARTreeRecoveryUnit;

/**
 *  A RecordStore that stores all data in-memory.
 */
class ARTreeRecordStore : public RecordStore {
public:
    explicit ARTreeRecordStore(const StringData& ns, const StringData& ident, ARTree* artree);
    ~ARTreeRecordStore();

    const char* name() const override;

    RecordData dataFor(OperationContext* opCtx, const RecordId& loc) const override;

    bool findRecord(OperationContext* opCtx, const RecordId& loc, RecordData* rd) const override;

    void deleteRecord(OperationContext* opCtx, const RecordId& dl) override;

    StatusWith<RecordId> insertRecord(OperationContext* opCtx,
                                      const char* data,
                                      int len,
                                      bool enforceQuota) override;

    StatusWith<RecordId> insertRecord(OperationContext* opCtx,
                                      const DocWriter* doc,
                                      bool enforceQuota) override;

    StatusWith<RecordId> updateRecord(OperationContext* opCtx,
                                      const RecordId& oldLocation,
                                      const char* data,
                                      int len,
                                      bool enforceQuota,
                                      UpdateNotifier* notifier) override;

    bool updateWithDamagesSupported() const override;

    StatusWith<RecordData> updateWithDamages(OperationContext* opCtx,
                                             const RecordId& loc,
                                             const RecordData& oldRec,
                                             const char* damageSource,
                                             const mutablebson::DamageVector& damages) override;

    std::unique_ptr<SeekableRecordCursor> getCursor(OperationContext* opCtx,
                                                    bool isForward = true) const override;

    std::vector<std::unique_ptr<RecordCursor>> getManyCursors(
        OperationContext* opCtx) const override;

    Status truncate(OperationContext* opCtx) override;

    Status validate(OperationContext* opCtx,
                    bool full,
                    bool scanData,
                    ValidateAdaptor* adaptor,
                    ValidateResults* results,
                    BSONObjBuilder* output) override;

    void appendCustomStats(OperationContext* opCtx,
                           BSONObjBuilder* result,
                           double scale) const override;

    Status touch(OperationContext* opCtx, BSONObjBuilder* output) const override;

    int64_t storageSize(OperationContext* opCtx,
                        BSONObjBuilder* extraInfo = nullptr,
                        int infoLevel = 0) const override;

    void setCappedCallback(CappedCallback*) override;
    long long dataSize(OperationContext*) const override;
    long long numRecords(OperationContext*) const override;

    void updateStatsAfterRepair(OperationContext* opCtx,
                                long long numRecords,
                                long long dataSize) override {}

    boost::optional<RecordId> oplogStartHack(OperationContext* opCtx,
                                             const RecordId& startingPosition) const override;

    void temp_cappedTruncateAfter(OperationContext* opCtx, RecordId end, bool inclusive) override;

    Status setCustomOption(OperationContext* opCtx,
                           const BSONElement& option,
                           BSONObjBuilder* info);

    void increaseStorageSize(OperationContext* opCtx, int size, bool enforceQuota);

    void setDataSize(uint64_t size);

    void setNumRecords(uint64_t count);

    bool isCapped() const {
        return _isCapped;
    }
    const ARTree* artree() const {
        return const_cast<const ARTree*>(_artree);
    }
    ARTree* getARTree() const {
        if (_isClosed)
            return nullptr;
        return _artree;
    }
    bool isClosed() const {
        return _isClosed;
    }
    StringData ident() {
        return _ident;
    }
    uint64_t cappedMaxSize() const {
        return _cappedMaxSize;
    }
    uint64_t cappedMaxDocs() const {
        return _cappedMaxDocs;
    }

    ARTree* getART() const {
        return _artree;
    }
    void close() {
        _isClosed = true;
    }

    friend class ARTreeRecordCursor;
    friend class ARTreeRecordReverseCursor;

private:
    class InsertChange;
    class DeleteChange;
    class UpdateChange;
    class TruncateChange;

    ARTreeRecoveryUnit* getRecoveryUnit(OperationContext* opCtx);
    ARTreeRecoveryUnit* getRecoveryUnitConst(OperationContext* opCtx) const;
    bool cappedOverflow();

    StringData _ident;
    ARTree* _artree;
    bool _isClosed;
    bool _isCapped;
    uint64_t _cappedMaxSize;
    uint64_t _cappedMaxDocs;
    CappedCallback* _cappedCallback;
};

class ARTreeRecordCursor : public SeekableRecordCursor {
public:
    ARTreeRecordCursor(OperationContext* opCtx, const ARTreeRecordStore& rs, bool tailable = false);
    ~ARTreeRecordCursor();

    boost::optional<Record> next() override;
    boost::optional<Record> seekExact(const RecordId& id) override;
    void save() override;
    bool restore() override;
    void detachFromOperationContext() override;
    void reattachToOperationContext(OperationContext* opCtx) override;

private:
    OperationContext* _opCtx;  // not owned
    bool _lastMoveWasRestore = false;
    const ARTreeRecordStore& _rs;
    bool _isClosed;
    bool _tailable;
    RecordId _curr;
    ARTreeIterator _iterator[1];
};

class ARTreeRecordReverseCursor : public SeekableRecordCursor {
public:
    ARTreeRecordReverseCursor(OperationContext* opCtx, const ARTreeRecordStore& rs);
    ~ARTreeRecordReverseCursor();

    boost::optional<Record> next() override;
    boost::optional<Record> seekExact(const RecordId& id) override;
    void save() override;
    bool restore() override;
    void detachFromOperationContext() override;
    void reattachToOperationContext(OperationContext* opCtx) override;

private:
    OperationContext* _opCtx;  // not owned
    const ARTreeRecordStore& _rs;
    bool _isClosed;
    RecordId _lastLoc;
    RecordId _savedLoc;
    bool _reverseEof;
    ARTreeIterator _iterator[1];
};

}  // namespace mongo
