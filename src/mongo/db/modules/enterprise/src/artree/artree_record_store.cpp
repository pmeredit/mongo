//@file artree_records.cpp
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
#include "artree_record_store.h"

#include <iostream>

#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/oplog_hack.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/stacktrace.h"

#include "artree.h"
#include "artree_debug.h"
#include "artree_nodes.h"
#include "artree_records.h"
#include "artree_recovery_unit.h"
#include "artree_util.h"

namespace mongo {

class ARTreeRecordStore::InsertChange : public RecoveryUnit::Change {
public:
    InsertChange(OperationContext* opCtx, ARTreeRecordStore* rs, RecordId loc)
        : _opCtx(opCtx), _rs(rs), _loc(loc) {}

    virtual void commit() {
        // the main action of insert commit: set txn timestamp of the new doc
        ARTree* artree = _rs->_artree;
        artree->commitInsertRec(_loc.repr());

        if (_rs->isCapped())
            artree->dequeueUncommittedRecord(_loc.repr());
    }

    virtual void rollback() {
        ARTreeRecoveryUnit* recUnit = _rs->getRecoveryUnit(_opCtx);
        uint32_t set = recUnit->getSet();

        if (_rs->isCapped())
            _rs->_artree->dequeueUncommittedRecord(_loc.repr());

        // the main action of insert rollback: delete the added document
        _rs->_artree->deleteRec(set, _loc.repr());
    }

private:
    OperationContext* _opCtx;
    ARTreeRecordStore* _rs;
    const RecordId _loc;
};

class ARTreeRecordStore::DeleteChange : public RecoveryUnit::Change {
public:
    DeleteChange(OperationContext* opCtx, ARTreeRecordStore* rs, RecordId loc)
        : _opCtx(opCtx), _rs(rs), _loc(loc) {}

    virtual void commit() {
        // assign a deadstamp t the record id
        ARTree* artree = _rs->_artree;
        artree->assignDeadstampId(_loc.repr());

        // on commit remove the record
        ARTreeRecoveryUnit* recUnit = _rs->getRecoveryUnit(_opCtx);
        uint32_t set = recUnit->getSet();
        artree->deleteRec(set, _loc.repr());

        if (!_rs->isCapped()) {
            ARTSlot slot[1];
            slot->bits = 0;

            // reclaim old record: place old record on the tail of the waiting recId frame
            slot->off = _loc.repr();
            artree->addSlotToFrame(&artree->_headRecId[set], &artree->_tailRecId[set], slot);
        }
    }

    virtual void rollback() {
        // on rollback clear all dead bits
        ARTree* artree = _rs->_artree;
        ARTRecord* rec = artree->fetchRec(_loc.repr());
        rec->_deadstamp = 0;
    }

private:
    OperationContext* _opCtx;
    ARTreeRecordStore* _rs;
    const RecordId _loc;
};

class ARTreeRecordStore::UpdateChange : public RecoveryUnit::Change {
public:
    UpdateChange(OperationContext* opCtx, ARTreeRecordStore* rs, RecordId loc, uint64_t old)
        : _opCtx(opCtx), _rs(rs), _loc(loc), _old(old) {}

    virtual void commit() {
        // main action of update commit: set txn timestamp
        ARTree* artree = _rs->_artree;
        ARTreeRecoveryUnit* recUnit = _rs->getRecoveryUnit(_opCtx);
        uint32_t set = recUnit->getSet();
        artree->commitUpdateRec(set, _loc.repr(), _old);
        recUnit->resetTxnId();
    }

    virtual void rollback() {
        ARTree* artree = _rs->_artree;
        ARTreeRecoveryUnit* recUnit = _rs->getRecoveryUnit(_opCtx);
        uint32_t set = recUnit->getSet();
        artree->unUpdateRec(set, _loc.repr());  // this will compute the negative delta
    }

private:
    OperationContext* _opCtx;
    ARTreeRecordStore* _rs;
    const RecordId _loc;
    uint64_t _old;
    uint64_t _version;
};

//
// RecordStore
//

ARTreeRecordStore::ARTreeRecordStore(const StringData& ns, const StringData& ident, ARTree* artree)
    : RecordStore(ns),
      _ident(ident),
      _artree(artree),
      _isClosed(false),
      _isCapped(artree->_cappedCollection),
      _cappedMaxSize(artree->_cappedMaxSize),
      _cappedMaxDocs(artree->_cappedMaxDocs) {

    _cappedCallback = nullptr;
    if (artree)
        ++artree->_nClients;
}

ARTreeRecordStore::~ARTreeRecordStore() {
    // clear cursors
    if (!_isClosed && _artree && !--_artree->_nClients) {
        _artree->close();
        _isClosed = true;
    }
}

const char* ARTreeRecordStore::name() const {
    return "inMemory";
}

RecordData ARTreeRecordStore::dataFor(OperationContext* opCtx, const RecordId& loc) const {
    if (FUNCTION_TRACE)
        log() << "ARTReeRecordStore::" << __FUNCTION__ << " loc = " << loc.repr();

    if (!_artree->isValidRecId(loc.repr())) {
        log() << "ARTReeRecordStore::" << __FUNCTION__ << " : invalid loc passed to 'dataFor'";
        return {};
    }

    const ARTDocument* doc = _artree->fetchDoc(loc.repr());
    // XXX handle (doc == nullptr)
    return RecordData(doc->doc(), doc->_docLen);
}

bool ARTreeRecordStore::findRecord(OperationContext* opCtx,
                                   const RecordId& loc,
                                   RecordData* record) const {
    if (FUNCTION_TRACE)
        log() << "ARTReeRecordStore::" << __FUNCTION__ << " loc = " << loc.repr();

    if (!_artree->isValidRecId(loc.repr())) {
        log() << "ARTReeRecordStore::" << __FUNCTION__ << " : invalid loc passed to 'findRecord'";
        return false;
    }

    // followVersionChain here

    const ARTDocument* doc = _artree->fetchDoc(loc.repr());
    if (nullptr == doc)
        return false;  // might not be a visible RecordId
    *record = RecordData(doc->doc(), doc->_docLen);
    return true;
}

void ARTreeRecordStore::deleteRecord(OperationContext* opCtx, const RecordId& loc) {
    if (FUNCTION_TRACE)
        log() << "ARTReeRecordStore::" << __FUNCTION__ << " loc = " << loc.repr();

    if (!_artree->isValidRecId(loc.repr())) {
        log() << "ARTReeRecordStore::" << __FUNCTION__ << " : invalid loc passed to 'deleteRecord'";
        return;
    }

    ARTRecord* rec = _artree->fetchRec(loc.repr());
    MutexSpinLock::lock(rec->_mutex);

    // code added : 2015.09.07
    ARTreeRecoveryUnit* recUnit = ARTree::recUnit(opCtx);
    uint64_t ts = recUnit->getTxnId();

    // allow for delete following update within same transaction
    bool isLocked = rec->_opCtx && rec->_opCtx == opCtx;

    if (!isLocked) {
        if (rec->_deadstamp || rec->_timestamp > ts || !rec->_basever || !(rec->_timestamp & 1)) {
            MutexSpinLock::unlock(rec->_mutex);

            if (CONFLICT_TRACE) {
                log() << "ARTReeRecordStore::" << __FUNCTION__ << ":" << __LINE__
                      << " : throw WriteConflictException";
                if (rec->_deadstamp)
                    log() << "  rec->_deadstamp = " << rec->_deadstamp;
                else if (rec->_timestamp > ts)
                    log() << "  rec->_timestamp > ts : " << rec->_timestamp << " > " << ts;
                else if (!rec->_basever)
                    log() << "  rec->_basever = " << rec->_basever;
                else if (!(rec->_timestamp & 1))
                    log() << " isReader(rec->_timestamp) = " << !(rec->_timestamp & 1);
            }

            // do we need this:
            // recUnit->abandonSnapshot();
            // recUnit->resetTxnId();

            throw WriteConflictException();
        }
    }

    rec->_deadstamp = 2;  // uncommitted record, lowest reader possible
    MutexSpinLock::unlock(rec->_mutex);
    // end new code

    opCtx->recoveryUnit()->registerChange(new DeleteChange(opCtx, this, loc));
}

inline bool ARTreeRecordStore::cappedOverflow() {
    return (_cappedMaxSize < _artree->_docSize ||
            (_cappedMaxDocs && _cappedMaxDocs < _artree->_docCount));
}

ARTreeRecoveryUnit* ARTreeRecordStore::getRecoveryUnit(OperationContext* opCtx) {
    return reinterpret_cast<ARTreeRecoveryUnit*>(opCtx->recoveryUnit());
    // TODO: figure out how to replace with 'checked_cast'
}

ARTreeRecoveryUnit* ARTreeRecordStore::getRecoveryUnitConst(OperationContext* opCtx) const {
    return ARTree::recUnit(opCtx);
}

StatusWith<RecordId> ARTreeRecordStore::insertRecord(OperationContext* opCtx,
                                                     const char* data,
                                                     int len,
                                                     bool enforceQuota) {
    if (FUNCTION_TRACE)
        log() << "ARTReeRecordStore::" << __FUNCTION__ << ":" << __LINE__;

    const uint8_t* theData = (const uint8_t*)data;
    ARTreeRecoveryUnit* recUnit = getRecoveryUnit(opCtx);
    uint32_t set = recUnit->getSet();

    // if capped, shrink to fit doc count quota
    if (_isCapped) {
        while (_artree->_cappedMaxDocs <= _artree->_docCount) {
            _artree->cappedTrim(opCtx, set, _cappedCallback);
        }
    }
    RecordId loc(_artree->storeRec(set, theData, len, opCtx));

    // if capped, shrink to fit doc size quota
    if (_isCapped) {
        while (_artree->_cappedMaxSize < _artree->_docSize) {
            _artree->cappedTrim(opCtx, set, _cappedCallback);
        }
    }
    recUnit->registerChange(new InsertChange(opCtx, this, loc));
    return StatusWith<RecordId>(loc);
}

StatusWith<RecordId> ARTreeRecordStore::insertRecord(OperationContext* opCtx,
                                                     const DocWriter* doc,
                                                     bool enforceQuota) {
    const int len = doc->documentSize();
    auto buf = std::unique_ptr<char[]>(new char[len]);
    doc->writeDocument(buf.get());
    StatusWith<RecordId> loc = insertRecord(opCtx, buf.get(), len, enforceQuota);
    return loc;
}

StatusWith<RecordId> ARTreeRecordStore::updateRecord(OperationContext* opCtx,
                                                     const RecordId& loc,
                                                     const char* data,
                                                     int len,
                                                     bool enforceQuota,
                                                     UpdateNotifier* notifier) {
    if (FUNCTION_TRACE)
        log() << "ARTReeRecordStore::" << __FUNCTION__ << ":" << __LINE__
              << "  id = " << loc.repr();
    //<< "  data = " << BSONObj(data).toString(0,0);

    ARTreeRecoveryUnit* recUnit = getRecoveryUnit(opCtx);

    if (!_artree->isValidRecId(loc.repr())) {
        log() << "ARTReeRecordStore::" << __FUNCTION__ << " : invalid loc passed to 'updateRecord'";
        return StatusWith<RecordId>(ErrorCodes::InternalError,
                                    "invalid loc passed to 'updateRecord");
    }

    ARTRecord* rec = _artree->fetchRec(loc.repr());
    MutexSpinLock::lock(rec->_mutex);

    // dirty read?
    bool isLocked = (rec->_opCtx && rec->_opCtx == opCtx);

    const uint8_t* theData = (const uint8_t*)data;
    uint32_t set = recUnit->getSet();

    if (_artree->updateRec(opCtx, recUnit->getTxnId(), set, isLocked, theData, len, loc.repr())) {
        recUnit->registerChange(new UpdateChange(opCtx, this, loc, rec->_prevVersion.off));
        rec->_opCtx = opCtx;
    }

    MutexSpinLock::unlock(rec->_mutex);
    return StatusWith<RecordId>(loc);
}

bool ARTreeRecordStore::updateWithDamagesSupported() const {
    return false;
}

StatusWith<RecordData> ARTreeRecordStore::updateWithDamages(
    OperationContext* opCtx,
    const RecordId& loc,
    const RecordData& oldRec,
    const char* damageSource,
    const mutablebson::DamageVector& damages) {
    return Status::OK();
}

std::unique_ptr<SeekableRecordCursor> ARTreeRecordStore::getCursor(OperationContext* opCtx,
                                                                   bool isForward) const {
    ARTreeRecoveryUnit* recUnit = getRecoveryUnitConst(opCtx);
    recUnit->createSnapshotId();

    if (isForward) {
        return stdx::make_unique<ARTreeRecordCursor>(opCtx, *this, false);
    } else {
        return stdx::make_unique<ARTreeRecordReverseCursor>(opCtx, *this);
    }
}

std::vector<std::unique_ptr<RecordCursor>> ARTreeRecordStore::getManyCursors(
    OperationContext* opCtx) const {
    std::vector<std::unique_ptr<RecordCursor>> out;
    out.push_back(stdx::make_unique<ARTreeRecordCursor>(opCtx, *this));
    return out;
}

Status ARTreeRecordStore::truncate(OperationContext* opCtx) {
    _artree->truncate();
    return Status::OK();
}

void ARTreeRecordStore::temp_cappedTruncateAfter(OperationContext* opCtx,
                                                 RecordId end,
                                                 bool inclusive) {
    ARTreeRecordReverseCursor cursor(opCtx, *this);
    while (auto record = cursor.next()) {
        RecordId loc = record->id;
        if (end < loc || (inclusive && end == loc)) {
            WriteUnitOfWork wuow(opCtx);
            deleteRecord(opCtx, loc);
            wuow.commit();
        } else
            break;
    }
}

Status ARTreeRecordStore::validate(OperationContext* opCtx,
                                   bool full,
                                   bool scanData,
                                   ValidateAdaptor* adaptor,
                                   ValidateResults* results,
                                   BSONObjBuilder* output) {
    results->valid = true;
    long long nrecords = 0;
    long long dataSizeTotal = 0;

    {
        auto cur = getCursor(opCtx, true);
        boost::optional<Record> rec;
        while ((rec = cur->next())) {
            ++nrecords;
            if (full && scanData) {
                size_t dataSize;
                Status status = adaptor->validate(rec.value().data, &dataSize);
                if (!status.isOK()) {
                    results->valid = false;
                    results->errors.push_back(str::stream() << rec.value().id << " is corrupted");
                }
                dataSizeTotal += static_cast<long long>(dataSize);
            }
        }
    }

    if (full && scanData && results->valid) {
        if (nrecords != numRecords(opCtx) || dataSizeTotal != dataSize(opCtx)) {
            warning() << "Existing record and data size counters (" << numRecords(opCtx)
                      << " records " << dataSize(opCtx) << " bytes) "
                      << "are inconsistent with full validation results (" << nrecords
                      << " records " << dataSizeTotal << " bytes). "
                      << "Updating counters with new values.";
        }

        setNumRecords(nrecords);
        setDataSize(dataSizeTotal);
    }

    output->appendNumber("nrecords", nrecords);
    return Status::OK();
}

void ARTreeRecordStore::appendCustomStats(OperationContext* opCtx,
                                          BSONObjBuilder* result,
                                          double scale) const {
    if (!result)
        return;
    result->appendBool("capped", _isCapped);
    if (_isCapped) {
        if (0.0 == scale)
            scale = 1.0;
        result->appendIntOrLL("max", _cappedMaxDocs);
        result->appendIntOrLL("maxSize", static_cast<long long>(_cappedMaxSize / scale));
        result->appendIntOrLL("maxDocs", static_cast<int>(_cappedMaxDocs / scale));

        log() << "ARTRecordStore::" << __FUNCTION__ << ":" << __LINE__
              << " cappedMaxDocs = " << _cappedMaxDocs << " cappedMaxSize = " << _cappedMaxSize;

        // TODO: what exactly is needed here ?
        // result->appendIntOrLL("sleepCount", _cappedSleep);
        // result->appendIntOrLL("sleepMS", _cappedSleepMS);
    }
}

Status ARTreeRecordStore::touch(OperationContext* opCtx, BSONObjBuilder* output) const {
    return Status::OK();
}

Status ARTreeRecordStore::setCustomOption(OperationContext* opCtx,
                                          const BSONElement& option,
                                          BSONObjBuilder* info) {
    return Status::OK();
}

void ARTreeRecordStore::increaseStorageSize(OperationContext* opCtx, int size, bool enforceQuota) {
    invariant(!"increaseStorageSize not yet implemented");
}

void ARTreeRecordStore::setCappedCallback(CappedCallback* callback) {
    _cappedCallback = callback;
}

int64_t ARTreeRecordStore::storageSize(OperationContext* opCtx,
                                       BSONObjBuilder* extraInfo,
                                       int infoLevel) const {
    return dataSize(opCtx);
}

boost::optional<RecordId> ARTreeRecordStore::oplogStartHack(
    OperationContext* opCtx, const RecordId& startingPosition) const {
    return RecordId();
}

long long ARTreeRecordStore::dataSize(OperationContext* opCtx) const {
    return _artree->getSize();
}

long long ARTreeRecordStore::numRecords(OperationContext* opCtx) const {
    return _artree->getCount();
}

void ARTreeRecordStore::setDataSize(uint64_t size) {
    _artree->setSize(size);
}

void ARTreeRecordStore::setNumRecords(uint64_t count) {
    return _artree->setCount(count);
}

//
// Forward Cursor
//

ARTreeRecordCursor::ARTreeRecordCursor(OperationContext* opCtx,
                                       const ARTreeRecordStore& rs,
                                       bool tailable)
    : _opCtx(opCtx), _rs(rs), _isClosed(false), _tailable(tailable) {

    ARTree* artree = _rs.getARTree();
    _iterator->init(opCtx, artree, true /*forward*/);
    if (artree)
        ++artree->_nClients;
}

ARTreeRecordCursor::~ARTreeRecordCursor() {
    _iterator->deinit();
    if (!_isClosed) {
        _isClosed = true;
        ARTree* artree = _rs.getARTree();
        if (artree && !--artree->_nClients)
            artree->close();
    }
}

boost::optional<Record> ARTreeRecordCursor::next() {
    if (FUNCTION_TRACE)
        log() << "ARTReeRecordCursor::" << __FUNCTION__ << ":" << __LINE__;

    if (!_rs.getARTree()->_cappedCollection)
        if (_iterator->eof())
            return {};

    _iterator->start();

    RecordId loc(_iterator->next(false));

    if (_iterator->isDead())
        return {};
    if (_iterator->eof())
        return {};

    return {{loc, _rs.dataFor(_opCtx, loc)}};
}

boost::optional<Record> ARTreeRecordCursor::seekExact(const RecordId& recId) {
    if (FUNCTION_TRACE)
        log() << "ARTReeRecordCursor::" << __FUNCTION__ << " recId = " << recId.repr();

    // @@@ always use the base version as start point on seek
    uint64_t loc = recId.repr();
    ARTRecord* rec = _rs._artree->fetchRec(loc);
    if (!rec->_basever) {
        log() << "ARTReeRecordCursor::" << __FUNCTION__ << " loc = " << loc;
        loc = rec->_base.off;
    }

    uint64_t recno = loc;
    if (!_rs.getARTree()->_cappedCollection) {
        save();
        restore();
    }
    uint64_t recidx = _iterator->setRec(recno);

    /*
    log() << "ARTReeRecordCursor::" << __FUNCTION__ << ":" << __LINE__
        << "  input id = " << recId.repr()
        //<< ", base id = " << loc
        << ", setRec id = " << recidx;
    */

    RecordId loc2(recidx);

    if (_iterator->isDead() || !recidx)
        return {};

    return {{loc2, _rs.dataFor(_opCtx, loc2)}};
}

void ARTreeRecordCursor::save() {
    if (REC_UNIT_TRACE | FUNCTION_TRACE)
        log() << "ARTreeRecordCursor::" << __FUNCTION__ << ":" << __LINE__;
    _iterator->saveState();
}

bool ARTreeRecordCursor::restore() {
    if (REC_UNIT_TRACE | FUNCTION_TRACE)
        log() << "ARTreeRecordCursor::" << __FUNCTION__ << ":" << __LINE__;

    return _iterator->restoreState();
}

void ARTreeRecordCursor::detachFromOperationContext() {
    if (REC_UNIT_TRACE | FUNCTION_TRACE)
        log() << "ARTreeRecordCursor::" << __FUNCTION__ << ":" << __LINE__;
}

void ARTreeRecordCursor::reattachToOperationContext(OperationContext* opCtx) {
    if (REC_UNIT_TRACE | FUNCTION_TRACE)
        log() << "ARTreeRecordCursor::" << __FUNCTION__ << ":" << __LINE__;
}

//
// Reverse Cursor
//

ARTreeRecordReverseCursor::ARTreeRecordReverseCursor(OperationContext* opCtx,
                                                     const ARTreeRecordStore& rs)
    : _opCtx(opCtx), _rs(rs), _isClosed(false) {

    ARTree* artree = rs.getARTree();
    _iterator->init(opCtx, artree, false /*reverse*/);
    if (artree)
        ++artree->_nClients;
}

ARTreeRecordReverseCursor::~ARTreeRecordReverseCursor() {
    _iterator->deinit();
    if (!_isClosed) {
        _isClosed = true;
        ARTree* artree = _rs.getARTree();
        if (artree && !--artree->_nClients)
            artree->close();
    }
}

boost::optional<Record> ARTreeRecordReverseCursor::next() {
    if (!_rs.getARTree()->_cappedCollection)
        if (_iterator->eof())
            return {};

    _iterator->start();

    RecordId loc(_iterator->prev(false));
    if (_iterator->isDead())
        return {};  // rec;
    if (_iterator->eof())
        return {};

    return {{loc, _rs.dataFor(_opCtx, loc)}};
}

boost::optional<Record> ARTreeRecordReverseCursor::seekExact(const RecordId& recId) {
    uint64_t recno = recId.repr();
    uint64_t recidx = _iterator->setRec(recno);
    RecordId loc(recidx);

    if (_iterator->isDead() || !loc.repr())
        return {};

    return {{loc, _rs.dataFor(_opCtx, loc)}};
}

void ARTreeRecordReverseCursor::save() {
    _iterator->saveState();
}

bool ARTreeRecordReverseCursor::restore() {
    return _iterator->restoreState();
}

void ARTreeRecordReverseCursor::detachFromOperationContext() {}

void ARTreeRecordReverseCursor::reattachToOperationContext(OperationContext* opCtx) {
    if (REC_UNIT_TRACE)
        log() << "ARTreeRecordStore::" << __FUNCTION__ << ":" << __LINE__;
}

}  // namespace mongo
