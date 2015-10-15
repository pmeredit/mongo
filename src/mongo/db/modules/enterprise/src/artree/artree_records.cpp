//@file artree_records.cpp
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

//
// Some of this is derived code.  The original code is in the public domain:
//    ARTful5: Adaptive Radix Trie key-value store
//    Author: Karl Malbrain, malbrain@cal.berkeley.edu
//    Date:   13 JAN 15
//

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/capped_callback.h"
#include "mongo/db/storage/record_data.h"
#include "mongo/platform/basic.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

#include "artree.h"
#include "artree_debug.h"
#include "artree_index.h"
#include "artree_nodes.h"
#include "artree_records.h"
#include "artree_util.h"

#include <iostream>
#include <stdlib.h>
#include <string.h>

namespace mongo {

//
//  Write document in collection
//  update slot with allocation.
//
ARTDocument* ARTree::storeDocument(
    ARTSlot* slot, uint32_t set, const uint8_t* data, uint32_t len, bool isDoc) {
    uint32_t amt = len + sizeof(ARTDocument);
    uint32_t bits = 3;

    while ((1UL << bits) < amt)
        bits++;

    ARTAddr addr = arenaAllocStorage(set, bits);
    ARTDocument* doc = static_cast<ARTDocument*>(arenaSlotAddr(addr));

    if (data)
        memcpy(doc->_document, data, len);

    doc->_docLen = len;

    slot->off = addr.off;
    slot->nbits = bits;

    if (isDoc) {
        _docSize += len;
        _docCount++;
    }

    return doc;
}

void ARTree::cappedTrim(OperationContext* opCtx, uint32_t set, CappedCallback* callback) {
    ARTRecord* rec = fetchRec(++_cappedBase);
    ARTDocument* doc = fetchDoc(rec->_doc);
    _docSize -= doc->_docLen;
    _docCount--;

    // free the associated document

    if (rec->_doc->nbits) {
        ARTDocument* doc = fetchDoc(rec);
        RecordData recData(doc->doc(), doc->docLen());

        if (callback)
            uassertStatusOK(callback->aboutToDeleteCapped(opCtx, RecordId(_cappedBase), recData));

        arenaFreeStorage(set, rec->_doc);
        rec->_doc->bits = 0;
    }

    rec->_used = false;
}

//
//  Write document in collection
//  return new RecordId
//
uint64_t ARTree::storeRec(uint32_t set,
                          const uint8_t* data,
                          uint32_t len,
                          OperationContext* opCtx) {
    if (FUNCTION_TRACE)
        log() << "ARTree::" << __FUNCTION__ << ":" << __LINE__ << " set = " << set
              << ", len = " << len;
    //<< ", data = " << ARTree::bsonStr((const char*)data);

    ARTAddr recordId = allocRecId(set);
    ARTRecord* rec = fetchRec(recordId);
    MutexSpinLock::lock(rec->_mutex);

    // atomically install the new document
    ARTSlot slot[1];
    slot->bits = 0;
    storeDocument(slot, set, data, len, STORE_DOC);
    rec->_doc->bits = slot->bits;

    rec->_timestamp = 2;  // any reader will skip uncommitted
    rec->_basever = true;
    rec->_used = true;

    // store opCtx for dirty reads
    rec->_opCtx = opCtx;

    MutexSpinLock::unlock(rec->_mutex);
    return recordId.off;
}

//
//  update document in collection
//  return true if UOW entry needed
//
bool ARTree::updateRec(OperationContext* opCtx,
                       uint64_t ts,
                       uint32_t set,
                       bool isLocked,
                       const uint8_t* data,
                       uint32_t len,
                       uint64_t loc) {
    if (FUNCTION_TRACE)
        log() << "ARTree::" << __FUNCTION__ << ":" << __LINE__ << " set = " << set
              << ", ts = " << ts << ", loc = " << loc << ", len = " << len;
    //<< ", data = " << ARTree::bsonStr((const char*)data);

    ARTAddr recId;
    recId.off = loc;

    ARTRecord* rec = fetchRec(recId);

    if (_cappedCollection) {
        ARTDocument* doc = fetchDoc(rec);
        if (rec->_oldDoc->off == 0) {
            storeDocument(rec->_oldDoc, set, doc->_document, doc->_docLen, false);
        }
        if (len + sizeof(ARTDocument) <= (1ULL << rec->_doc->nbits)) {
            memcpy(doc->_document, data, len);
            doc->_docLen = len;
        } else {  // document doesn't fit
            ARTSlot slot[1];
            slot->bits = 0;
            storeDocument(slot, set, data, len, false);
            rec->_doc->bits = slot->bits;
        }
        return true;
    }

    //  if updating an uncommitted record from the same recovery unit
    //  just go ahead and update the document

    if (isLocked && !isCommitted(rec->_timestamp)) {
        ARTDocument* originalDoc = fetchDoc(rec->_doc);

        // need to free up the doc storage first, then overwrite
        ARTSlot slot[1];
        slot->bits = rec->_doc->bits;
        arenaFreeStorage(set, slot);

        slot->bits = 0;

        if (data)
            storeDocument(slot, set, data, len, STORE_DOC);
        // installs correct offset reference into 'slot'

        rec->_doc->bits = slot->bits;
        _docSize -= originalDoc->_docLen;
        return false;  // no - use a new unit of work item
    }

    /*
     *  (1) case: _deadstamp < ts
     *          locked for delete by uncommitted transaction
     *          or committed deletion
     *  (2) case: _timestamp > ts
     *          not visible in snapshot
     *  (3) case: _basever = false
     *          not a base version of the record
     *  (4) case: reader _timestamp e.g. not committed
     *          updater about to write with ts = k,
     *          reader arrives with ts = k+1,
     *          reader skips record,
     *          updater installs ts = k.
     */

    if (!_catalogCollection) {
        if (rec->_deadstamp               // (1)
            || rec->_timestamp > ts       // (2)
          //|| !rec->_basever             // (3)
            || isReader(rec->_timestamp)  // (4)
            ) {

            if (CONFLICT_TRACE) {
                log() << "ARTree::" << __FUNCTION__ << ":" << __LINE__
                      << " : throw WriteConflictException";
                if (rec->_deadstamp)
                    log() << "  rec->_deadstamp = " << rec->_deadstamp;
                else if (rec->_timestamp > ts)
                    log() << "  rec->_timestamp > ts : " << rec->_timestamp << " > " << ts;
                else if (!rec->_basever)
                    log() << "  rec->_basever = " << rec->_basever;
                else if (isReader(rec->_timestamp))
                    log() << "  isReader(rec->_timestamp) = " << rec->_timestamp;
            }

            ARTreeRecoveryUnit* recUnit = dynamic_cast<ARTreeRecoveryUnit*>(opCtx->recoveryUnit());
            recUnit->abandonSnapshot();  // TODO: can we remove this?
            recUnit->resetTxnId();

            MutexSpinLock::unlock(rec->_mutex);
            throw WriteConflictException();
        }
    }

    ARTAddr prevId = allocRecId(set);
    ARTRecord* prevRec = fetchRec(prevId);

    // move old doc to new location
    prevRec->_timestamp = rec->_timestamp;
    prevRec->_prevVersion = rec->_prevVersion;
    prevRec->_base = recId;
    prevRec->_doc->bits = rec->_doc->bits;
    prevRec->_version = rec->_version;
    *prevRec->_mutex = 0;
    prevRec->_used = true;

    // synchronize access to prev before current
    std::atomic_thread_fence(std::memory_order_seq_cst);

    // atomically install the new doc
    rec->_prevVersion = prevId;  // now two copies of old doc
    rec->_timestamp = 2;         // any reader will skip uncommitted record

    ARTSlot slot[1];
    slot->bits = 0;

    if (data) {
        storeDocument(slot, set, data, len, STORE_DOC);
        // installs correct offset reference into 'slot'
    }

    rec->_doc->bits = slot->bits;
    rec->_used = true;
    rec->_version++;
    rec->_opCtx = opCtx;

    // update stats
    ARTDocument* doc = fetchDoc(prevRec->_doc);
    _docSize -= doc->_docLen;  //(1ULL << slot->nbits);
    _docCount--;

    // unlock and return
    return true;  // make a UOW item
}

//
//  commit insert document change
//
void ARTree::commitInsertRec(uint64_t loc) {
    ARTRecord* rec = fetchRec(loc);
    assignTxnId(rec);
}

//
//  commit update document change
//
void ARTree::commitUpdateRec(uint32_t set, uint64_t loc, uint64_t old) {
    ARTAddr recId;
    recId.off = loc;

    ARTRecord* rec = fetchRec(recId);
    MutexSpinLock::lock(rec->_mutex);

    assignTxnId(rec);

    if (_cappedCollection) {
        addSlotToFrame(&_headFreeStorage[set], &_tailFreeStorage[set], rec->_oldDoc);
        rec->_oldDoc->bits = 0;
        MutexSpinLock::unlock(rec->_mutex);
        return;
    }

    // reclaim old record:
    //   place the old record on the tail of the waiting recId frame
    //   i.e. allocated new rec onto free list

    ARTSlot slot[1];
    slot->bits = 0;
    slot->off = old;
    addSlotToFrame(&_headRecId[set], &_tailRecId[set], slot);

    MutexSpinLock::unlock(rec->_mutex);
}

//
//  un-update document in collection
//
void ARTree::unUpdateRec(uint32_t set, uint64_t loc) {
    ARTAddr recId;
    recId.off = loc;

    ARTRecord* rec = fetchRec(recId);
    MutexSpinLock::lock(rec->_mutex);

    if (_cappedCollection) {
        addSlotToFrame(&_headFreeStorage[set], &_tailFreeStorage[set], rec->_doc);
        rec->_doc->bits = rec->_oldDoc->bits;
        MutexSpinLock::unlock(rec->_mutex);
        return;
    }

    ARTAddr prevId = rec->_prevVersion;
    ARTRecord* prevRec = fetchRec(prevId);

    // move prev doc to original location
    // need to free up the doc storage first, before overwriting
    ARTSlot slot[1];
    slot->bits = rec->_doc->bits;
    ARTDocument* uncommittedDoc = fetchDoc(rec->_doc);
    arenaFreeStorage(set, slot);
    rec->_doc->bits = prevRec->_doc->bits;

    rec->_version = prevRec->_version;
    rec->_used = true;
    rec->_timestamp = prevRec->_timestamp;
    rec->_prevVersion = prevRec->_prevVersion;
    rec->_opCtx = nullptr;

    // reclaim un-updated record:
    ARTDocument* originalDoc = fetchDoc(rec->_doc);
    uint64_t deltaSize = originalDoc->_docLen - uncommittedDoc->_docLen;
    _docSize += deltaSize;

    MutexSpinLock::unlock(rec->_mutex);
}

void ARTree::deleteRec(uint32_t set, uint64_t loc) {
    ARTAddr recId;
    recId.off = loc;
    ARTRecord* rec = fetchRec(recId);
    assignLatestTimestamp(&rec->_deadstamp);
    rec->_opCtx = nullptr;
    ARTDocument* doc = fetchDoc(rec->_doc);
    _docSize -= doc->_docLen;
    _docCount--;
}

//
// return document from slot
//
ARTDocument* ARTree::fetchDoc(ARTSlot* slot) const {
    if (!slot->bits)
        return nullptr;

    return static_cast<ARTDocument*>(arenaSlotAddr(slot));
}
//
// return document for recId
//
ARTDocument* ARTree::fetchDoc(ARTAddr recId) const {
    ARTRecord* rec = fetchRec(recId);
    if (!rec || !rec->_used)
        return nullptr;

    return static_cast<ARTDocument*>(arenaSlotAddr(rec->_doc));
}

ARTDocument* ARTree::fetchDoc(uint64_t loc) const {
    ARTAddr recId;
    recId.off = loc;
    return fetchDoc(recId);
}

ARTDocument* ARTree::fetchDoc(ARTRecord* rec) const {
    if (!rec->_used)
        return nullptr;
    return static_cast<ARTDocument*>(arenaSlotAddr(rec->_doc));
}

//
// increment a segmented recordId
//
bool ARTree::incr(ARTAddr* recId) {
    ARTAddr start = *recId;

    if (_cappedCollection) {
        // check valid range (cappedBase, cappedRec]
        if (recId->off < _cappedBase || _cappedRec <= recId->off)
            return false;

        ++recId->off;
        return true;
    }

    while (recId->_segment <= _currentArena) {
        if (++recId->_offset <= _arena[recId->_segment]->_arenaRec._offset)
            return true;

        recId->_offset = 0;
        recId->_segment++;
    }

    *recId = start;
    return false;
}

//
// decrement a segmented recordId
//
bool ARTree::decr(ARTAddr* recId) {
    ARTAddr start = *recId;

    if (_cappedCollection) {
        if (--recId->off <= _cappedBase || _cappedRec < recId->off) {
            ++recId->off;
            return false;
        }
        return true;
    }

    while (recId->_offset) {
        if (--recId->_offset)
            return true;
        if (!recId->_segment)
            break;

        recId->_segment--;
        recId->_offset = _arena[recId->_segment]->_arenaRec._offset + 1;
    }

    *recId = start;
    return false;
}

//
// return record for recId
//
ARTRecord* ARTree::fetchRec(uint64_t loc) const {
    ARTAddr recId;
    recId.off = loc;
    return fetchRec(recId);
}

ARTRecord* ARTree::fetchRec(ARTAddr recId) const {
    if (!recId._offset)
        return nullptr;

    if (_cappedCollection) {
        uint32_t off = 1 + (recId.off - 1) % _cappedMaxDocs;
        ARTRecord* rec = _arena[0]->_records - off;
        return rec;
    }

    // non-capped case

    ARTRecord* rec = _arena[recId._segment]->_records - recId._offset;
    return rec;
}

//
// return true if visible, else false
//
/*
bool ARTree::isVisibleVersion(void* opCtx, uint64_t txnId, ARTAddr recId) {
    ARTRecord* rec = fetchRec(recId);

    // dirty read, return immediately
    if (opCtx && rec->_opCtx == opCtx)
        return true;

    if (_cappedCollection)
        return true;

    if (!rec->_used)
        return false;

    // uncommitted delete transactions are visible
    if (isReader(rec->_deadstamp) || txnId < rec->_deadstamp)
        //if (!isReader(rec->_timestamp) && txnId >= rec->_timestamp)
        //if (isReader(rec->_timestamp) || txnId < rec->_timestamp)
            return true;

    return false;
}
*/

bool ARTree::isVisibleVersion(void* opCtx, uint64_t txnId, ARTAddr recId) {
    ARTRecord* rec = fetchRec(recId);

    // dirty read, return immediately
    if (opCtx && rec->_opCtx == opCtx)
        return true;

    if (_cappedCollection)
        return true;

    if (!rec->_used)
        return false;

    // reject committed updates/deletes
    if (isCommitted(rec->_deadstamp) && txnId >= rec->_deadstamp)
        return false;

    // we require insert transactions to be committed for read
    if (isCommitted(rec->_timestamp) && txnId >= rec->_timestamp)
        return true;

    return false;
}

//
// follow version chain till visible and not deleted rec found
// return rec if found, else nullptr
//
ARTAddr ARTree::followVersionChain(void* opCtx, uint64_t txnId, ARTAddr recId) {
    // step through version chain
    ARTRecord* rec;
    ARTAddr zero;
    zero.off = 0;

    while ((rec = fetchRec(recId))) {
        // dirty read, return immediately
        if (opCtx && rec->_opCtx == opCtx)
            return recId;

        if (_cappedCollection)
            return recId;

        if (VERSION_TRACE)
            log() << "ARTree::" << __FUNCTION__ << ":" << __LINE__ << "\n  txnId = " << txnId
                  << "\n  rec->_deadstamp = " << rec->_deadstamp
                  << "\n  rec->_timestamp = " << rec->_timestamp;

        // reject committed updates/deletes
        if (isCommitted(rec->_deadstamp) && txnId >= rec->_deadstamp)
            return zero;

        // we require insert transactions to be committed for read
        if (isCommitted(rec->_timestamp) && txnId >= rec->_timestamp) {
            /* proposed new code 2015.09.07
            uint64_t ts;
            while (isReader(ts = rec->_deadstamp) && ts < txnId)
                ARTreeCompareAndSwap(&rec->_deadstamp, ts, txnId); */

            break;
        }

        // Note: for any version other than the current version
        // the visibility window for any prior version equals
        // ts <= txnId < previous version ts (which is successor in list)
        recId = rec->_prevVersion;
    }

    if (!rec)
        return zero;

    // read time was acquired later than your commit time
    uint64_t ts;
    while (isReader(ts = rec->_timestamp) && ts < txnId)
        ARTreeCompareAndSwap(&rec->_timestamp, ts, txnId);

    if (!rec->_doc->off)
        return zero;

    return recId;
}

//
// Assign the latest writer timestamp to some
//   rec->_timestamp or rec->_deadstamp
// (which may be concurrently accessed).
//
void ARTree::assignLatestTimestamp(uint64_t* timestamp) {
    // assign reader timestamp if not yet assigned
    if (!*timestamp)
        *timestamp = _engine->allocTxnId(_engine->en_reader);

    while (true) {
        // possibly a scan has started and loaded a subsequent
        // ts, in which case we need a newer timestamp

        uint64_t txnId = _engine->allocTxnId(_engine->en_writer);
        uint64_t ts = *timestamp;

        if (ts > txnId)
            continue;

        if (ARTreeCompareAndSwap(timestamp, ts, txnId) == ts)
            break;
    }
}

//
//  assign txnid to record
//
uint64_t ARTree::assignTxnId(ARTRecord* rec) {
    // clear dirty read bits
    rec->_opCtx = nullptr;

    // assign the next writer timestamp
    assignLatestTimestamp(&rec->_timestamp);

    /*
    if (!rec->_prevVersion.off) return rec->_timestamp;
    ARTRecord* prev = fetchRec(rec->_prevVersion);
    prev->_deadstamp = rec->_timestamp;
    */

    return rec->_timestamp;
}

//
//  assign deadstamp to record
//
void ARTree::assignDeadstampId(uint64_t loc) {
    ARTAddr recId;
    recId.off = loc;

    ARTRecord* rec = fetchRec(recId);

    // assign reader timestamp
    rec->_deadstamp = _engine->allocTxnId(_engine->en_reader);

    while (true) {
        // possibly another scan has already seen the rec and loaded its
        // subsequent deadstamp, in which case we need a newer deadstamp

        uint64_t txnId = _engine->allocTxnId(_engine->en_writer);
        uint64_t ts = rec->_deadstamp;

        if (ts && ts > txnId)
            continue;

        if (ARTreeCompareAndSwap(&rec->_deadstamp, ts, txnId) == ts)
            break;
    }
}

//
// queue record onto open transaction queue
//
void ARTree::enqueueUncommittedRecord(uint64_t loc) {
    ARTRecord* rec = fetchRec(loc);
    _uncommittedRecs->insert(rec->_queue, loc);
}

//
// dequeue record from open transaction queue
//
void ARTree::dequeueUncommittedRecord(uint64_t loc) {
    ARTAddr recId;
    recId.off = loc;
    ARTRecord* rec = fetchRec(recId);
    _uncommittedRecs->remove(rec->_queue);
}

//
// allocate next available record id
//
ARTAddr ARTree::allocRecId(uint32_t set) {
    ARTAddr recId;

    if (_cappedCollection) {
        recId.off = ++_cappedRec;
        ARTRecord* rec = fetchRec(recId);
        memset(rec, 0, sizeof(ARTRecord));

        // immediately place on openTxn Queue
        _uncommittedRecs->insert(rec->_queue, recId.off);

        return recId;
    }

    ARTSlot* queue = &_freeRecId[set];
    MutexSpinLock::lock(queue);
    ARTSlot slot[1];

    // allocate initial empty frame
    if (!queue->off)
        queue->addr = arenaAlloc(sizeof(ARTFrame));

    // see if there is a free record in the free queue
    while (!getSlotFromFrame(queue, slot)) {
        if (!processRecIdQueue(set, queue, &_tailRecId[set])) {
            MutexSpinLock::lock(
                _arenaMutex);  // TODO: see if this can be replaced with raii pattern
            uint64_t max = _arena[_currentArena]->_arenaMax -
                _arena[_currentArena]->_arenaRec._offset * sizeof(ARTRecord);

            uint32_t dup = FrameSlots;

            ARTFrame* frame = (ARTFrame*)arenaSlotAddr(queue);
            queue->nslot = 0;
            slot->bits = 0;

            if (_arenaNext._offset * 8ULL > max - dup * sizeof(ARTRecord))
                arenaExtend(0);

            // allocate a batch of record-ids

            uint64_t o = (_arena[_currentArena]->_arenaRec.off += dup);
            MutexSpinLock::unlock(_arenaMutex);

            while (dup--) {
                frame->_slots[queue->nslot++].off = o - (FrameSlots - dup) + 1;
            }
        }
    }

    recId.off = slot->off;
    ARTRecord* rec = fetchRec(recId);

    if (rec == nullptr)
        log() << "ARTree::" << __FUNCTION__ << ":" << __LINE__ << " : fetchRec(" << recId.off
              << ") => nullptr";
    else
        memset(rec, 0, sizeof(ARTRecord));

    MutexSpinLock::unlock(queue);
    return recId;
}

bool ARTree::isValidRecId(uint64_t loc) const {
    ARTAddr recId;
    recId.off = loc;

    if (_cappedCollection) {
        if (_cappedRec < recId.off || recId.off <= _cappedBase)
            return false;
        else {
            ARTRecord* rec = fetchRec(loc);
            return rec->_used;
        }
    }

    if (recId._segment <= _currentArena)
        if (recId._offset <= _arena[recId._segment]->_arenaRec._offset) {
            ARTRecord* rec = fetchRec(loc);
            return rec->_used;
        }

    return false;
}

//
// see if "oldest" storage delete time is newer than oldest cursor
//
bool ARTree::processStorageQueue(uint32_t set, ARTSlot* queue, ARTSlot* tail) {
    if (!tail->off)
        return false;

    // tryLock here, if locked, return
    // no point contending on free list maintenance

    MutexSpinLock::lock(tail);
    ARTFrame* frame2 = (ARTFrame*)arenaSlotAddr(tail);

    if (!tail->off || frame2->_timestamp >= _cursors->getMinimum()) {
        MutexSpinLock::unlock(tail);
        return false;
    }

    // wait time has expired, so we can
    // pull empty storage off of wait queue

    for (uint32_t idx = 0; idx < tail->nslot; idx++) {
        ARTSlot* slot = &frame2->_slots[idx];
        arenaFreeStorage(set, slot);
    }

    MutexSpinLock::unlock(tail);  // @@@ added 2015.09.29 [paul]
    return false;
}

//
// see if "oldest" record delete time is newer than oldest cursor
//
bool ARTree::processRecIdQueue(uint32_t set, ARTSlot* queue, ARTSlot* tail) {
    if (!tail->off)
        return false;

    MutexSpinLock::lock(tail);
    ARTFrame* frame2 = (ARTFrame*)arenaSlotAddr(tail);

    if (!tail->off || frame2->_timestamp >= _cursors->getMinimum()) {
        MutexSpinLock::unlock(tail);
        return false;
    }

    // wait time has expired, so we can put
    // current empty frame of RecId on free frame list
    putFrameOnFreeList(arenaFrameAddr(queue), queue->addr);

    // process frame of deleted records from tail
    // and make that the frame of free RecId instead
    for (uint32_t idx = 0; idx < tail->nslot; idx++) {
        ARTAddr recId;
        recId.off = frame2->_slots[idx].off;
        processRecordEntry(recId, set);
    }

    //  switch empty frame of free recId to old tail frame
    queue->nslot = tail->nslot;
    queue->off = tail->off;

    // remove old tail and compute new tail slot

    ARTFrame* frame3 = (ARTFrame*)arenaSlotAddr(frame2->_prev);
    frame3->_next->bits = 0;

    //  is this the last frame in the prev chain?

    if (frame3->_prev->off)
        tail->bits = frame2->_prev->bits;
    else
        tail->bits = 0;

    MutexSpinLock::unlock(tail);
    return true;
}

//
// return the document storage to the free storage
// list
//
// Note precondition: recId valid
//
void ARTree::processRecordEntry(ARTAddr recId, uint32_t set) {
    ARTRecord* rec = fetchRec(recId);

    ARTSlot slot[1];
    slot->bits = 0;
    slot->bits = rec->_doc->bits;
    memset(rec, 0, sizeof(ARTRecord));

    if (slot->nbits > 3)
        arenaFreeStorage(set, slot);
}

//
// add storage node to free frame
//
void ARTree::arenaFreeStorage(uint32_t set, ARTSlot* slot) {
    uint32_t bits = slot->nbits;

    if (bits > 16)
        set = 0;

    addSlotToFrame(&_freeStorage[set][bits], nullptr, slot);
}

}  // namespace mongo
