// artree_oplog_store.h

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
#include "artree_record_store.h"
#include "artree_iterator.h"

namespace mongo {

class ARTreeRecordCursor;
class ARTreeRecordReverseCursor;

class ARTOplog {
public:
    enum OplogState {
        opsRegistered = 0,
        opsUncommitted = 1,
        opsCommitted = 2,
        opsRolledBack = 3,
        opsDeleted = 4,
    };

    ARTSlot _next[1];  // node state is kept in _next->nslot
    ARTSlot _prev[1];
    ARTSlot _doc[1];
    RecordId _loc;
};

/**
 *  A RecordStore that implements the oplog as a radix tree
 */
class ARTreeOplogStore : public ARTreeRecordStore {
public:
    explicit ARTreeOplogStore(const StringData& ns, const StringData& ident, ARTree* artree);

    virtual ~ARTreeOplogStore();

    virtual Status oplogDiskLocRegister(OperationContext* txn, const Timestamp& opTime);

    virtual RecordData dataFor(OperationContext* opCtx, const RecordId& loc) const;

    virtual bool findRecord(OperationContext* opCtx, const RecordId& loc, RecordData* rd) const;

    virtual void deleteRecord(OperationContext* opCtx, const RecordId& dl);

    virtual StatusWith<RecordId> insertRecord(OperationContext* opCtx,
                                              const char* data,
                                              int len,
                                              bool enforceQuota);

    virtual StatusWith<RecordId> insertRecord(OperationContext* opCtx,
                                              const DocWriter* doc,
                                              bool enforceQuota);

    virtual StatusWith<RecordId> updateRecord(OperationContext* opCtx,
                                              const RecordId& oldLocation,
                                              const char* data,
                                              int len,
                                              bool enforceQuota,
                                              UpdateNotifier* notifier);

    virtual std::unique_ptr<SeekableRecordCursor> getCursor(OperationContext* opCtx,
                                                            bool isForward = true) const;

    virtual std::vector<std::unique_ptr<RecordCursor>> getManyCursors(
        OperationContext* opCtx) const;

    virtual boost::optional<RecordId> oplogStartHack(OperationContext* opCtx,
                                                     const RecordId& startingPosition) const;

    bool isCapped() const {
        return true;
    }

private:
    /*
     * convert optime (recId) to a key, placing result in 'key'
     * @return length of key
     *
     * Note: may be redundant with: KeyString(RecordId rid)
     */
    static uint32_t recId2key(uint8_t* key, uint32_t max, uint64_t recId);

    /*
     * @return the ARTree index associated with this oplog
     */
    ARTreeIndex* getIndex() const {
        return _index;
    }

    /*
     * @return pointer to oplog node corresponding to a given optime recId
     */
    ARTOplog* fetchOplog(uint64_t recId) const;

    /*
     * @return tail of oplog
     */
    ARTSlot* getTail() const {
        return (ARTSlot*)_tail;
    }

    /*
     * @return head of oplog
     */
    ARTSlot* getHead() const {
        return (ARTSlot*)_head;
    }

    /*
     * helper function to insert documents
     */
    StatusWith<RecordId> insertRecord(OperationContext* opCtx,
                                      ARTSlot* docSlot,
                                      uint32_t set,
                                      bool enforceQuota);

    friend class ARTreeOplogCursor;
    friend class ARTreeOplogReverseCursor;

    class InsertChange;
    class UpdateChange;
    class TruncateChange;

    ARTreeIndex* _index;
    ARTSlot _head[1];
    ARTSlot _tail[1];
};

class ARTreeCursor;

class ARTreeOplogCursor : public SeekableRecordCursor {
public:
    ARTreeOplogCursor(OperationContext* opCtx, const ARTreeOplogStore& rs);

    virtual boost::optional<Record> next();
    virtual boost::optional<Record> seekExact(const RecordId& id);
    virtual void save();
    virtual bool restore();
    virtual void detachFromOperationContext();
    virtual void reattachToOperationContext(OperationContext* opCtx);

private:
    OperationContext* _opCtx;  // not owned
    const ARTreeOplogStore& _rs;
    bool _alreadyPositioned;
    ARTOplog* _oplog;
};

class ARTreeOplogReverseCursor : public SeekableRecordCursor {
public:
    ARTreeOplogReverseCursor(OperationContext* opCtx, const ARTreeOplogStore& rs);

    virtual boost::optional<Record> next();
    virtual boost::optional<Record> seekExact(const RecordId& id);
    virtual void save();
    virtual bool restore();
    virtual void detachFromOperationContext();
    virtual void reattachToOperationContext(OperationContext* opCtx);

private:
    OperationContext* _opCtx;  // not owned
    const ARTreeOplogStore& _rs;
    bool _alreadyPositioned;
    ARTOplog* _oplog;
};

}  // namespace mongo
