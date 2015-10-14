// artree_oplog_store.cpp

/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
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

#include <iostream>

#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/oplog_hack.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#include "artree.h"
#include "artree_cursor.h"
#include "artree_debug.h"
#include "artree_nodes.h"
#include "artree_oplog_store.h"
#include "artree_records.h"
#include "artree_record_store.h"
#include "artree_recovery_unit.h"
#include "artree_util.h"

namespace mongo {

// convert recId to key
//
uint32_t ARTreeOplogStore::recId2key(uint8_t* key, uint32_t max, uint64_t recId) {
    for (uint32_t idx = sizeof(RecordId); idx--;) {
        key[idx] = recId & 0xff;
        recId >>= 8;
    }
    return sizeof(RecordId);
}

ARTOplog* ARTreeOplogStore::fetchOplog(uint64_t recId) const {
    uint8_t key[sizeof(RecordId)];
    uint32_t keylen = recId2key(key, sizeof(key), recId);

    ARTSlot* slot = _index->findKey(nullptr, _index->_root, key, keylen);

    if (!slot || slot->type != Optime) {
        if (OPLOG_TRACE)
            log() << "ARTreeOplogStore::" << __FUNCTION__ << recId << " -- not found -- ";
        return nullptr;
    }

    if (OPLOG_TRACE)
        log() << "ARTreeOplogStore::" << __FUNCTION__ << recId << " -- found -- ";

    ARTree* art = _index->_art;
    return (ARTOplog*)(art->arenaSlotAddr(slot));
}

class ARTreeOplogStore::InsertChange : public RecoveryUnit::Change {
public:
    InsertChange(OperationContext* opCtx, ARTreeOplogStore* rs, RecordId loc, ARTSlot* docSlot)
        : _opCtx(opCtx), _rs(rs), _loc(loc) {
        if (OPLOG_TRACE)
            log() << "ARTreeOplogStore::Change::" << __FUNCTION__ << _loc.repr();

        _docSlot->bits = docSlot->bits;
    }

    virtual void commit() {
        if (OPLOG_TRACE)
            log() << "ARTreeOplogStore::Change::" << __FUNCTION__ << _loc.repr();

        ARTOplog* e = _rs->fetchOplog(_loc.repr());
        if (e)
            e->_next->state = ARTOplog::opsCommitted;
    }

    virtual void rollback() {
        if (OPLOG_TRACE)
            log() << "ARTreeOplogStore::InsertChange::" << __FUNCTION__ << ":" << __LINE__;

        ARTOplog* e = _rs->fetchOplog(_loc.repr());
        if (e)
            e->_next->state = ARTOplog::opsRolledBack;
    }

private:
    OperationContext* _opCtx;
    ARTreeOplogStore* _rs;
    const RecordId _loc;
    ARTSlot _docSlot[1];
};

class ARTreeOplogStore::UpdateChange : public RecoveryUnit::Change {
public:
    UpdateChange(OperationContext* opCtx, ARTreeOplogStore* rs, RecordId loc, ARTSlot* docSlot)
        : _opCtx(opCtx), _rs(rs), _loc(loc) {
        if (OPLOG_TRACE)
            log() << "ARTreeOplogStore::UpdateChange::" << __FUNCTION__ << ":" << __LINE__
                  << " loc = " << _loc.repr();

        _docSlot->bits = docSlot->bits;
    }

    virtual void commit() {
        if (OPLOG_TRACE)
            log() << "ARTreeOplogStore::UpdateChange::" << __FUNCTION__ << ":" << __LINE__
                  << " loc = " << _loc.repr();

        ARTOplog* e = _rs->fetchOplog(_loc.repr());
        ARTree* artree = _rs->getARTree();
        artree->_docSize += (1UL << _docSlot->nbits);
        artree->_docSize -= (1UL << e->_doc->nbits);
        uint32_t set = artree->arenaNodeSet();
        artree->addSlotToFrame(
            &artree->_headFreeStorage[set], &artree->_tailFreeStorage[set], e->_doc);
        e->_doc->bits = _docSlot->bits;
    }

    virtual void rollback() {
        if (OPLOG_TRACE)
            log() << "ARTreeOplogStore::UpdateChange::" << __FUNCTION__ << ":" << __LINE__
                  << " loc = " << _loc.repr();

        ARTree* artree = _rs->getARTree();
        uint32_t set = artree->arenaNodeSet();
        artree->addSlotToFrame(
            &artree->_headFreeStorage[set], &artree->_tailFreeStorage[set], _docSlot);
    }

private:
    OperationContext* _opCtx;
    ARTreeOplogStore* _rs;
    const RecordId _loc;
    ARTSlot _docSlot[1];
};

//
// Oplog RecordStore
//

ARTreeOplogStore::ARTreeOplogStore(const StringData& ns, const StringData& ident, ARTree* artree)
    : ARTreeRecordStore(ns, ident, artree) {
    _index = new ARTreeIndex(artree);
    _head->bits = 0;
    _tail->bits = 0;
}

ARTreeOplogStore::~ARTreeOplogStore() {}

Status ARTreeOplogStore::oplogDiskLocRegister(OperationContext* txn, const Timestamp& opTime) {
    StatusWith<RecordId> res = oploghack::keyForOptime(opTime);
    RecordId loc = res.getValue();

    if (OPLOG_TRACE)
        log() << "ARTreeOplogStore::" << __FUNCTION__ << ":" << __LINE__ << " loc = " << loc.repr();

    uint8_t key[sizeof(RecordId)];
    uint32_t keylen = recId2key(key, sizeof(key), loc.repr());
    uint32_t set = getARTree()->arenaNodeSet();

    ARTree* art = _index->_art;

    // linked list management
    // Note: locking is managed upstream
    ARTAddr addr = art->arenaAllocNode(set, Optime);
    ARTOplog* c = (ARTOplog*)(art->arenaSlotAddr(addr));
    c->_prev->off = _head->off;
    c->_loc = loc;

    if (_head->off) {
        ARTOplog* h = (ARTOplog*)(art->arenaSlotAddr(_head));
        h->_next->off = addr.off;
    }

    if (!_tail->off)
        _tail->off = addr.off;

    _head->off = addr.off;

    // index insertion
    ARTSlot slot[1];
    slot->bits = 0;
    slot->type = Optime;
    slot->off = addr.off;

    ARTSlot* leafSlot = _index->insertKey(_index->_root, set, key, keylen);
    leafSlot->bits = slot->bits;
    return Status::OK();
}

RecordData ARTreeOplogStore::dataFor(OperationContext* opCtx, const RecordId& loc) const {
    ARTOplog* e = fetchOplog(loc.repr());
    if (!e)
        return RecordData();

    ARTree* art = _index->_art;
    ARTDocument* doc = art->fetchDoc(e->_doc);
    return RecordData(doc->doc(), doc->_docLen);
}

bool ARTreeOplogStore::findRecord(OperationContext* opCtx,
                                  const RecordId& loc,
                                  RecordData* record) const {
    ARTree* art = _index->_art;

    // reset the transaction id
    // TODO: why are we doing this?
    // ARTreeRecoveryUnit* recUnit = dynamic_cast<ARTreeRecoveryUnit*>(opCtx->recoveryUnit());
    // recUnit->getEngine()->resetTxnId();

    ARTOplog* e = fetchOplog(loc.repr());
    if (!e)
        return false;

    ARTDocument* doc = art->fetchDoc(e->_doc);
    *record = RecordData(doc->doc(), doc->_docLen);
    return true;
}

void ARTreeOplogStore::deleteRecord(OperationContext* opCtx, const RecordId& loc) {}

StatusWith<RecordId> ARTreeOplogStore::insertRecord(OperationContext* opCtx,
                                                    ARTSlot* docSlot,
                                                    uint32_t set,
                                                    bool enforceQuota) {
    ARTDocument* doc = getARTree()->fetchDoc(docSlot);
    const char* theData = (char*)doc->_document;
    StatusWith<RecordId> s = oploghack::extractKey(theData, doc->_docLen);
    if (!s.getStatus().isOK())
        return s;
    RecordId loc = s.getValue();

    if (OPLOG_TRACE)
        log() << "ARTreeOplogStore::" << __FUNCTION__ << ":" << __LINE__ << " (" << loc.repr()
              << ")";

    ARTree* artree = getARTree();

    // note: removed resetTxnId [2015.09.13]

    ARTOplog* e = fetchOplog(loc.repr());
    if (!e)
        return StatusWith<RecordId>(ErrorCodes::InternalError, "Unregistered optime");
    e->_doc->bits = docSlot->bits;

    while (getARTree()->_cappedMaxSize < getARTree()->_docSize ||
           getARTree()->_cappedMaxDocs < getARTree()->_docCount) {
        MutexSpinLock::lock(artree->_oplogMutex);
        ARTSlot slot[1];

        slot->bits = getTail()->bits;
        ARTOplog* e = (ARTOplog*)(artree->arenaSlotAddr(_tail));
        _tail->bits = e->_next->bits;

        uint8_t key[sizeof(RecordId)];
        uint32_t keylen = recId2key(key, sizeof(key), e->_loc.repr());
        _index->deleteDocKey(set, opCtx, key, keylen, nullptr);

        ARTOplog* t = (ARTOplog*)(artree->arenaSlotAddr(_tail));
        t->_prev->bits = 0;

        artree->_docSize -= (1UL << e->_doc->nbits);
        artree->_docCount--;

        artree->addSlotToFrame(
            &artree->_headFreeStorage[set], &artree->_tailFreeStorage[set], e->_doc);
        MutexSpinLock::unlock(artree->_oplogMutex);
    }

    opCtx->recoveryUnit()->registerChange(new InsertChange(opCtx, this, loc, docSlot));
    return StatusWith<RecordId>(loc);
}

StatusWith<RecordId> ARTreeOplogStore::insertRecord(OperationContext* opCtx,
                                                    const char* data,
                                                    int len,
                                                    bool enforceQuota) {
    ARTSlot docSlot[1];
    docSlot->bits = 0;

    uint32_t set = getARTree()->arenaNodeSet();
    const uint8_t* theData = (const uint8_t*)data;

    getARTree()->storeDocument(docSlot, set, theData, len, true);
    return insertRecord(opCtx, docSlot, set, enforceQuota);
}

StatusWith<RecordId> ARTreeOplogStore::insertRecord(OperationContext* opCtx,
                                                    const DocWriter* docWrt,
                                                    bool enforceQuota) {
    if (OPLOG_TRACE)
        log() << "ARTOplogStore::" << __FUNCTION__ << ":" << __LINE__;

    ARTSlot docSlot[1];
    docSlot->bits = 0;

    const int len = docWrt->documentSize();
    uint32_t set = getARTree()->arenaNodeSet();
    ARTDocument* doc = getARTree()->storeDocument(docSlot, set, nullptr, len, true);
    docWrt->writeDocument((char*)doc->_document);
    return insertRecord(opCtx, docSlot, set, enforceQuota);
}

StatusWith<RecordId> ARTreeOplogStore::updateRecord(OperationContext* opCtx,
                                                    const RecordId& loc,
                                                    const char* data,
                                                    int len,
                                                    bool enforceQuota,
                                                    UpdateNotifier* notifier) {
    if (OPLOG_TRACE)
        log() << "ARTreeOplogStore::" << __FUNCTION__ << ":" << __LINE__ << " loc = " << loc.repr();

    // note: removed resetTxnId [2015.09.13]

    ARTSlot docSlot[1];
    uint32_t set = getARTree()->arenaNodeSet();
    getARTree()->storeDocument(docSlot, set, (const uint8_t*)data, len, true);

    opCtx->recoveryUnit()->registerChange(new UpdateChange(opCtx, this, loc, docSlot));
    return StatusWith<RecordId>(loc);
}

std::unique_ptr<SeekableRecordCursor> ARTreeOplogStore::getCursor(OperationContext* opCtx,
                                                                  bool isForward) const {
    if (OPLOG_TRACE)
        log() << "ARTreeOplogStore::" << __FUNCTION__ << "@" << (uint64_t)opCtx->recoveryUnit();

    // note: removed resetTxnId [2015.09.13]

    if (isForward)
        return stdx::make_unique<ARTreeOplogCursor>(opCtx, *this);
    else
        return stdx::make_unique<ARTreeOplogReverseCursor>(opCtx, *this);
}

std::vector<std::unique_ptr<RecordCursor>> ARTreeOplogStore::getManyCursors(
    OperationContext* opCtx) const {
    // note: removed resetTxnId [2015.09.13]

    std::vector<std::unique_ptr<RecordCursor>> out;
    out.push_back(stdx::make_unique<ARTreeOplogCursor>(opCtx, *this));
    return out;
}

boost::optional<RecordId> ARTreeOplogStore::oplogStartHack(OperationContext* opCtx,
                                                           const RecordId& startingPosition) const {
    // Return the RecordId of an oplog entry as close to startingPosition
    // as possible without being higher. If there are no entries <=
    // startingPosition,
    // return RecordId().

    if (OPLOG_TRACE)
        log() << "ARTreeOplogStore::" << __FUNCTION__ << ":" << __LINE__
              << " startingPosition = " << startingPosition.repr();

    // note: removed resetTxnId [2015.09.13]

    ARTreeCursor cursor[1] = {ARTreeCursor(_index)};
    uint8_t key[sizeof(RecordId)];
    uint32_t keylen = ARTreeOplogStore::recId2key(key, sizeof(key), startingPosition.repr());
    ARTSlot* slot = cursor->_index->findKey(cursor, _index->_root, key, keylen);
    if (!slot || !cursor->prevKey(false))
        return RecordId();

    return RecordId(cursor->_recordId.off);
}

//
// Forward Cursor
//

ARTreeOplogCursor::ARTreeOplogCursor(OperationContext* opCtx, const ARTreeOplogStore& rs)
    : _opCtx(opCtx), _rs(rs), _alreadyPositioned(false), _oplog(nullptr) {}

boost::optional<Record> ARTreeOplogCursor::next() {
    if (OPLOG_TRACE)
        log() << "ARTreeOplogCursor::" << __FUNCTION__ << ":" << __LINE__;

    // note: removed resetTxnId [2015.09.13]

    bool tryAgain;
    do {
        tryAgain = false;
        ARTree* art = _rs.getARTree();

        if (!_alreadyPositioned) {
            // follow linked list
            if (!_oplog) {
                if (!_rs.getTail()->off)
                    return {};
                else
                    _oplog = (ARTOplog*)(art->arenaSlotAddr(_rs.getTail()));
            } else {
                if (!_oplog->_next->off)
                    return {};
                else
                    _oplog = (ARTOplog*)art->arenaSlotAddr(_oplog->_next);
            }

            _alreadyPositioned = true;
        }

        if (_oplog->_next->state == ARTOplog::opsRolledBack) {
            _alreadyPositioned = false;
            tryAgain = true;
        }
    } while (tryAgain);

    if (_oplog->_next->state != ARTOplog::opsCommitted)
        return {};  // and _alreadyPositioned == true

    Record result;
    ARTree* art = _rs.getARTree();
    ARTDocument* doc = art->fetchDoc(_oplog->_doc);
    result.data = RecordData(doc->doc(), doc->_docLen);
    result.id = _oplog->_loc;

    if (OPLOG_TRACE)
        log() << "ARTreeOplogCursor::" << __FUNCTION__ << ":" << __LINE__ << " --> "
              << result.id.repr();

    _alreadyPositioned = false;
    return result;
}

// seek and fetch
// TODO: check never seeks uncommitted or rolledback
//
boost::optional<Record> ARTreeOplogCursor::seekExact(const RecordId& loc) {
    if (OPLOG_TRACE)
        log() << "ARTreeOplogCursor::" << __FUNCTION__ << ":" << __LINE__ << "( " << loc.repr()
              << " )";

    // note: removed resetTxnId [2015.09.13]

    ARTree* art = _rs.getARTree();
    ARTOplog* tail = (ARTOplog*)(art->arenaSlotAddr(_rs.getTail()));
    if (loc.repr() < tail->_loc.repr())
        return {};

    _oplog = _rs.fetchOplog(loc.repr());
    _alreadyPositioned = false;  // 'next' returns next record

    if (!_oplog)
        return {};

    Record result;
    ARTDocument* doc = art->fetchDoc(_oplog->_doc);
    result.data = RecordData(doc->doc(), doc->_docLen);
    result.id = _oplog->_loc;
    return result;
}

void ARTreeOplogCursor::save() {
    if (OPLOG_TRACE)
        log() << "ARTreeOplogCursor::" << __FUNCTION__;
}

bool ARTreeOplogCursor::restore() {
    if (OPLOG_TRACE)
        log() << "ARTreeOplogCursor::" << __FUNCTION__;
    return true;
}

void ARTreeOplogCursor::detachFromOperationContext() {
    if (OPLOG_TRACE)
        log() << "ARTreeOplogCursor::" << __FUNCTION__;
}

void ARTreeOplogCursor::reattachToOperationContext(OperationContext* opCtx) {
    if (OPLOG_TRACE)
        log() << "ARTreeOplogCursor::" << __FUNCTION__;
}

// TODO: consolidate this with forward cursor

//
// Reverse Cursor
//

ARTreeOplogReverseCursor::ARTreeOplogReverseCursor(OperationContext* opCtx,
                                                   const ARTreeOplogStore& rs)
    : _opCtx(opCtx), _rs(rs), _alreadyPositioned(false) {
    _oplog = nullptr;
}

boost::optional<Record> ARTreeOplogReverseCursor::next() {
    if (OPLOG_TRACE)
        log() << "ARTreeOplogReverseCursor::" << __FUNCTION__ << ":" << __LINE__;

    // note: removed resetTxnId [2015.09.13]
    ARTree* art = _rs.getARTree();

    bool tryAgain;
    do {
        tryAgain = false;

        if (!_alreadyPositioned) {
            if (!_oplog) {
                if (!_rs.getHead()->off)
                    return {};
                else
                    _oplog = (ARTOplog*)(art->arenaSlotAddr(_rs.getHead()));
            } else {
                if (!_oplog->_prev->off)
                    return {};
                else
                    _oplog = (ARTOplog*)art->arenaSlotAddr(_oplog->_prev);
            }

            _alreadyPositioned = true;
        }

        if (_oplog->_next->state == ARTOplog::opsRolledBack) {
            _alreadyPositioned = false;
            tryAgain = true;
        }

    } while (tryAgain);

    if (_oplog->_next->state != ARTOplog::opsCommitted)
        return {};

    Record result;
    ARTDocument* doc = art->fetchDoc(_oplog->_doc);
    result.data = RecordData(doc->doc(), doc->_docLen);
    result.id = _oplog->_loc;

    if (OPLOG_TRACE)
        log() << "ARTreeOplogReverseCursor::" << __FUNCTION__ << ":" << __LINE__ << " --> "
              << result.id.repr();

    _alreadyPositioned = false;
    return result;
}

boost::optional<Record> ARTreeOplogReverseCursor::seekExact(const RecordId& id) {
    if (OPLOG_TRACE)
        log() << "ARTreeOplogReverseCursor::" << __FUNCTION__;

    // note: removed resetTxnId [2015.09.13]

    ARTree* art = _rs.getARTree();
    ARTOplog* tail = (ARTOplog*)(art->arenaSlotAddr(_rs.getTail()));
    if (id.repr() < tail->_loc.repr())
        return {};

    _oplog = _rs.fetchOplog(id.repr());
    _alreadyPositioned = false;

    if (!_oplog)
        return {};

    Record result;
    ARTDocument* doc = art->fetchDoc(_oplog->_doc);
    result.data = RecordData(doc->doc(), doc->_docLen);
    result.id = _oplog->_loc;
    return result;
    return {};
}

void ARTreeOplogReverseCursor::save() {
    if (OPLOG_TRACE)
        log() << "ARTreeOplogReverseCursor::" << __FUNCTION__;
}

bool ARTreeOplogReverseCursor::restore() {
    if (OPLOG_TRACE)
        log() << "ARTreeOplogReverseCursor::" << __FUNCTION__;
    return true;
}

void ARTreeOplogReverseCursor::detachFromOperationContext() {
    if (OPLOG_TRACE)
        log() << "ARTreeOplogReverseCursor::" << __FUNCTION__;
}

void ARTreeOplogReverseCursor::reattachToOperationContext(OperationContext* opCtx) {
    if (OPLOG_TRACE)
        log() << "ARTreeOplogReverseCursor::" << __FUNCTION__;
}

}  // namespace mongo
