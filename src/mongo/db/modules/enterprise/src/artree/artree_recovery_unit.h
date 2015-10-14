// artree_recovery_unit.h
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

#include <set>
#include <vector>

#include "mongo/db/record_id.h"
#include "mongo/db/storage/recovery_unit.h"

namespace mongo {

class ARTreeKVEngine;

class ARTreeRecoveryUnit : public RecoveryUnit {
public:
    ARTreeRecoveryUnit(ARTreeKVEngine* engine);
    virtual ~ARTreeRecoveryUnit();

    void beginUnitOfWork(OperationContext* opCtx) final;
    void commitUnitOfWork() final;
    void abortUnitOfWork() final;
    void registerChange(Change* change) final;
    void* writingPtr(void* data, size_t len) final {
        MONGO_UNREACHABLE;
    }
    void setRollbackWritesDisabled() final {}
    bool waitUntilDurable() final;
    void abandonSnapshot() final;

    SnapshotId getSnapshotId() const final {
        return SnapshotId(_snapshotId);
    }

    void createSnapshotId() {
        if (_advanceSnapshotId)
            _snapshotId++;
        _advanceSnapshotId = false;
    }

    void resetTxnId();

    void clearQueue() {
        memset(_queue, 0, sizeof(ARTreeOrderedList::Entry));
    }

    uint64_t getTxnId() {
        return _queue->_value;
    }

    uint32_t getSet() const {
        return _set;
    }

    ARTreeKVEngine* getEngine() {
        return _engine;
    }

private:
    std::atomic_uint_fast64_t _snapshotId;  // current mongodb snapshot id
    bool _advanceSnapshotId;                // advance snapshot id on next read request
    std::vector<Change*> _changes;          // vector of uncommitted changes
    ARTreeKVEngine* _engine;                // parent KV engine (i.e.) the database
    ARTreeOrderedList::Entry _queue[1];     // defines lowest transaction id potentially available
    uint32_t _set;                          // allocation group id
};

}  // namespace mongo
