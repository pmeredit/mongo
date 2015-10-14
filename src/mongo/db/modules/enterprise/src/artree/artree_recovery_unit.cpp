// artree_recovery_unit.cpp

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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"
#include <typeinfo>

#include "mongo/util/log.h"
#include "mongo/db/storage/kv/kv_catalog.h"
#include "mongo/util/concurrency/thread_name.h"
#include "artree.h"
#include "artree_iterator.h"
#include "artree_kv_engine.h"
#include "artree_recovery_unit.h"
#include "artree_debug.h"
#include "artree.h"

namespace mongo {

ARTreeRecoveryUnit::ARTreeRecoveryUnit(ARTreeKVEngine* engine)
    : _snapshotId(1), _advanceSnapshotId(false) {
    // hash the thread name into the set
    // @@@ Is this needed at all??  Why are we doing this? [2015.09.30]
    std::string name = getThreadName();
    const char* p = name.c_str();
    uint32_t set = 0;
    while (*p) {
        set *= 17;
        set += *p++;
    }

    // alternatively (deleting the above):
    //   _set = sched_getcpu() % artree::nBuckets;
    // or
    //   implement this in 'getSet()'

    memset(_queue, 0, sizeof(ARTreeOrderedList::Entry));

    _set = set % artree::nBuckets;
    _engine = engine;
    resetTxnId();
}

ARTreeRecoveryUnit::~ARTreeRecoveryUnit() {
    // remove this transaction from the priority queue
    if (_queue->_value)
        _engine->dequeueTimestamp(_queue);
}

void ARTreeRecoveryUnit::registerChange(Change* change) {
    _changes.push_back(change);
}

void ARTreeRecoveryUnit::beginUnitOfWork(OperationContext* opCtx) {}

void ARTreeRecoveryUnit::commitUnitOfWork() {
    std::vector<Change*>::iterator it;
    std::vector<Change*>::iterator end;
    for (it = _changes.begin(), end = _changes.end(); it != end; ++it) {
        (*it)->commit();
    }
    resetTxnId();
    _changes.clear();
    _advanceSnapshotId = true;
}

void ARTreeRecoveryUnit::abortUnitOfWork() {
    _advanceSnapshotId = true;

    std::vector<Change*>::reverse_iterator it;
    std::vector<Change*>::reverse_iterator end;
    for (it = _changes.rbegin(), end = _changes.rend(); it != end; ++it) {
        (*it)->rollback();
    }
    _changes.clear();
}

bool ARTreeRecoveryUnit::waitUntilDurable() {
    return true;
}

void ARTreeRecoveryUnit::resetTxnId() {
    if (_queue->_value)
        _engine->dequeueTimestamp(_queue);
    _engine->enqueueTimestamp(_queue);
}

void ARTreeRecoveryUnit::abandonSnapshot() {
    if (FUNCTION_TRACE)
        log() << "ARTreeRecoveryUnit::abandonSnapshot()";

    //_advanceSnapshotId = true;
    ++_snapshotId;

    // resetTxnId();  // TODO: replace with a 'remove'
}
}
