//@file artree_sorted_data_impl.cpp
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

#include "mongo/platform/basic.h"
#include "artree_sorted_data_impl.h"

#include "mongo/bson/bsontypes.h"
#include "mongo/bson/ordering.h"
#include "mongo/db/storage/index_entry_comparison.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/util/log.h"

#include <iostream>
#include <cstdlib>
#include <cstring>

#include "artree_cursor.h"
#include "artree_debug.h"
#include "artree_nodes.h"
#include "artree_recovery_unit.h"
#include "artree_util.h"

namespace mongo {

class IndexChange : public RecoveryUnit::Change {
public:
    IndexChange(ARTreeIndex* index,
                OperationContext* opCtx,
                const BSONObj& bsonKey,
                RecordId loc,
                Ordering ordering,
                ARTSlot* slot,
                bool insert)
        : _index(index),
          _opCtx(opCtx),
          _bsonKey(bsonKey),
          _loc(loc),
          _ordering(ordering),
          _insert(insert) {
        _docSlot->bits = slot->bits;
    }

    // index is updated directly
    virtual void commit() {}

    // rollback removes the key, if any, recorded in this index change
    virtual void rollback() {
        if (_bsonKey.isEmpty())
            return;

        ARTreeRecoveryUnit* recUnit = ARTree::recUnit(_opCtx);
        uint32_t set = recUnit->getSet();
        KeyString key(_bsonKey, _ordering, _loc);

        if (_insert)
            _index->deleteDocKey(
                set, _opCtx, (const uint8_t*)key.getBuffer(), key.getSize(), _docSlot);
        else
            _index->insertDocKey(set,
                                 (const uint8_t*)key.getBuffer(),
                                 key.getSize(),
                                 true /*dupsAllowed*/,
                                 _docSlot);
    }

private:
    ARTreeIndex* _index;
    OperationContext* _opCtx;
    BSONObj _bsonKey;
    RecordId _loc;
    Ordering _ordering;
    ARTSlot _docSlot[1];
    bool _insert;
};

ARTreeSortedDataImpl::ARTreeSortedDataImpl(Ordering ordering, ARTreeIndex* index)
    : _ordering(ordering), _index(index), _unitTesting(false) {
    ++_index->_art->_nClients;
}

ARTreeSortedDataImpl::~ARTreeSortedDataImpl() {
    if (_index->_art && !--_index->_art->_nClients)
        _index->_art->close();
}

bool ARTreeSortedDataImpl::hasFieldNames(const BSONObj& obj) {
    BSONForEach(e, obj) {
        if (e.fieldName()[0])
            return true;
    }
    return false;
}

BSONObj ARTreeSortedDataImpl::stripFieldNames(const BSONObj& query) {
    if (!hasFieldNames(query))
        return query;
    BSONObjBuilder bb;
    BSONForEach(e, query) {
        bb.appendAs(e, StringData());
    }
    return bb.obj();
}

SortedDataBuilderInterface* ARTreeSortedDataImpl::getBulkBuilder(OperationContext* opCtx,
                                                                 bool dupsAllowed) {
    return new ARTreeBulkBuilder(opCtx, this, dupsAllowed);
}

Status ARTreeSortedDataImpl::insert(OperationContext* opCtx,
                                    const BSONObj& bsonKey,
                                    const RecordId& loc,
                                    bool dupsAllowed) {
    KeyString key(bsonKey, _ordering, loc);

    if (FUNCTION_TRACE)
        log() << "ARTreeSortedDataImpl::" << __FUNCTION__ << ":" << __LINE__
              << " loc = " << loc.repr() << ", KeyLen = " << key.getSize();

    ARTree* art = _index->_art;
    ARTreeRecoveryUnit* recUnit = ARTree::recUnit(opCtx);
    uint32_t set = recUnit->getSet();

    if (key.getSize() > MAX_KEYSIZE) {
        return Status(ErrorCodes::KeyTooLong, "Key too long");
    }

    ARTSlot slot[1];
    slot->bits = 0;
    slot->type = EndKey;

    art->storeDocument(
        slot, set, key.getTypeBits().getBuffer(), key.getTypeBits().getSize(), false /*isDoc*/);

    bool dupBit = _index->insertDocKey(
        set, (const uint8_t*)key.getBuffer(), key.getSize(), dupsAllowed, slot);

    if (dupBit) {
        std::ostringstream oss;
        oss << "E11000 : Duplicate key: " << bsonKey.toString(0, 0);
        return Status(ErrorCodes::DuplicateKey, oss.str());
    }

    opCtx->recoveryUnit()->registerChange(
        new IndexChange(_index, opCtx, bsonKey, loc, _ordering, slot, true /*insert*/));

    return Status::OK();
}

void ARTreeSortedDataImpl::unindex(OperationContext* opCtx,
                                   const BSONObj& bsonKey,
                                   const RecordId& loc,
                                   bool dupsAllowed) {
    if (FUNCTION_TRACE)
        log() << "ARTreeSortedDataImpl::" << __FUNCTION__ << ":" << __LINE__
              << " loc = " << loc.repr() << ", bsonKey = " << bsonKey.toString(0, 0);
    ARTreeRecoveryUnit* recUnit = ARTree::recUnit(opCtx);
    uint32_t set = recUnit->getSet();

    KeyString key(bsonKey, _ordering, loc);

    ARTSlot slot[1];
    _index->deleteDocKey(set, opCtx, (const uint8_t*)key.getBuffer(), key.getSize(), slot);

    /*
     TODO: replace this with:
        (1) change deleteDocKey to point to prior version of record, if any
        (2) put the key on the versioned record deleted-key list
        (3) put master record id into capped collection queue (value)
        (4) deleted-keys need to be actually removed during record reclaimation
    */

    opCtx->recoveryUnit()->registerChange(
        new IndexChange(_index, opCtx, bsonKey, loc, _ordering, slot, false /*insert*/));
}

Status ARTreeSortedDataImpl::dupKeyCheck(OperationContext* opCtx,
                                         const BSONObj& bsonKey,
                                         const RecordId& loc) {
    log() << "ARTreeSortedDataImpl::dupKeyCheck"
          << ":" << __LINE__ << "\n  loc = " << loc.repr()
          << "\n  key = " << bsonKey.toString(0, 0);

    bool found = false;
    auto cursor = newCursor(opCtx);
    auto kv = cursor->seek(bsonKey, true);
    if (!kv)
        return Status::OK();  // not found = no dup
    for (; kv; kv = cursor->next()) {
        if (loc == kv->loc) {
            if (found)  // already seen = dep
                return Status(ErrorCodes::DuplicateKey, "duplicate key");
            found = true;
        }
    }

    // found exactly one = no dup, else dup
    return found ? Status::OK() : Status(ErrorCodes::DuplicateKey, "duplicate key");
}

void ARTreeSortedDataImpl::fullValidate(OperationContext* opCtx,
                                        bool full,
                                        long long* numKeysOut,
                                        BSONObjBuilder* output) const {
    if (output)
        *output << "valid" << true;

    auto cursor = newCursor(opCtx);
    uint64_t count = 0;

    const auto requestedInfo = INDEX_TRACE ? Cursor::kKeyAndLoc : Cursor::kJustExistance;
    for (auto kv = cursor->seek(kMinBSONKey, true, requestedInfo); kv; kv = cursor->next()) {
        count++;
    }

    if (numKeysOut)
        *numKeysOut = count;

    // Nothing further to do if 'full' validation is not requested.
    if (!full)
        return;
    invariant(output);
}

bool ARTreeSortedDataImpl::appendCustomStats(OperationContext* opCtx,
                                             BSONObjBuilder* output,
                                             double scale) const {
    BSONObjBuilder metadata(output->subobjStart("metadata"));
    Status status = _index->getMetadata(opCtx, &metadata);
    if (!status.isOK()) {
        metadata.append("error", "unable to retrieve metadata");
        metadata.append("code", static_cast<int>(status.code()));
        metadata.append("reason", status.reason());
    }
    return true;
}

long long ARTreeSortedDataImpl::getSpaceUsedBytes(OperationContext* opCtx) const {
    return 0L;
}

bool ARTreeSortedDataImpl::isEmpty(OperationContext* opCtx) {
    return (_index->numEntries() == 0);
}

long long ARTreeSortedDataImpl::numEntries(OperationContext* opCtx) const {
    return _index->numEntries();
}

std::unique_ptr<SortedDataInterface::Cursor> ARTreeSortedDataImpl::newCursor(
    OperationContext* opCtx, bool isForward) const {
    return std::unique_ptr<SortedDataInterface::Cursor>(
        new ARTCursor(opCtx, _index, DEFAULT_STACK_MAX, isForward, _ordering));
}

Status ARTreeSortedDataImpl::initAsEmpty(OperationContext* opCtx) {
    return Status::OK();
}

//
// ARTreeIndexBuilder methods
//

ARTreeSortedDataBuilderImpl::ARTreeSortedDataBuilderImpl(OperationContext* opCtx,
                                                         ARTreeSortedDataImpl* artIndex,
                                                         bool dupsAllowed)
    : _opCtx(opCtx), _artIndex(artIndex), _dupsAllowed(dupsAllowed) {}

Status ARTreeSortedDataBuilderImpl::addKey(const BSONObj& key, const RecordId& loc) {
    return _artIndex->insert(_opCtx, key, loc, _dupsAllowed);
}

void ARTreeSortedDataBuilderImpl::commit(bool mayInterrupt) {
    WriteUnitOfWork uow(_opCtx);
    uow.commit();
}

//
// ARTreeBulkBuilder methods
//

ARTreeBulkBuilder::ARTreeBulkBuilder(OperationContext* opCtx,
                                     ARTreeSortedDataImpl* artIndex,
                                     bool dupsAllowed)
    : _opCtx(opCtx), _artIndex(artIndex), _dupsAllowed(dupsAllowed) {}

Status ARTreeBulkBuilder::addKey(const BSONObj& key, const RecordId& loc) {
    return _artIndex->insert(_opCtx, key, loc, _dupsAllowed);
}

void ARTreeBulkBuilder::commit(bool mayInterrupt) {
    WriteUnitOfWork uow(_opCtx);
    uow.commit();
}

//
// ARTCursor methods
//

ARTCursor::ARTCursor(OperationContext* opCtx,
                     ARTreeIndex* index,
                     uint32_t stackMax,
                     bool isForward,
                     const Ordering& ordering)
    : _artreeCursor(ARTreeCursor::newCursor(opCtx, index, stackMax)),
      _isForward(isForward),
      _ordering(ordering) {
    ARTreeRecoveryUnit* recUnit = ARTree::recUnit(opCtx);
    recUnit->resetTxnId();
    if (_artreeCursor->_index->_art)
        ++_artreeCursor->_index->_art->_nClients;
}

ARTCursor::~ARTCursor() {
    if (_artreeCursor->_index->_art && !--_artreeCursor->_index->_art->_nClients)
        ARTreeCursor::endCursor(_artreeCursor);
}

BSONObj ARTCursor::getKey() const {
    KeyString::TypeBits typeBits;
    ARTDocument* doc = reinterpret_cast<ARTDocument*>(_artreeCursor->getValue());
    BufReader bufReader(doc->_document, doc->_docLen);
    typeBits.resetFromBuffer(&bufReader);

    return KeyString::toBson(reinterpret_cast<const char*>(_artreeCursor->getKey()),
                             _artreeCursor->getKeyLen(),
                             _ordering,
                             typeBits);
}

RecordId ARTCursor::getRecordId() const {
    return RecordId(_artreeCursor->getRecordId().off);
}

int ARTCursor::getDirection() const {
    return _isForward ? +1 : -1;
}

bool ARTCursor::isEOF() const {
    return _artreeCursor->atEOF();
}

// cursor interface

/**
 *  Seeks to the provided end key and returns current position.
 *  Logically, end key is a sentinel equals to the first key
 *  greater than the valid range.
 */
void ARTCursor::setEndPosition(const BSONObj& bsonKey0, bool inclusive) {
    BSONObj bsonKey = ARTreeSortedDataImpl::stripFieldNames(bsonKey0);

    if (!bsonKey.isEmpty()) {
        KeyString key(bsonKey, _ordering, KeyString::kInclusive);

        _artreeCursor->_endKeySize = key.getSize();
        _artreeCursor->_endKeyInclusive = inclusive;
        memcpy(_artreeCursor->_endKey,
               reinterpret_cast<const uint8_t*>(key.getBuffer()),
               _artreeCursor->_endKeySize);
    } else {
        uint8_t key[2];
        key[0] = _isForward ? '\xf0' : '\x0a';
        key[1] = '\x04';

        if (CURSOR_TRACE)
            log() << "ARTCursor::" << __FUNCTION__ << ":" << __LINE__
                  << "end key = " << ARTree::keyStr(key, 2);

        _artreeCursor->_endKeySize = 2;
        _artreeCursor->_endKeyInclusive = false;
        memcpy(_artreeCursor->_endKey, key, 2);
    }
}

/**
 *  Seeks to the provided key and returns current position.
 *  Start key is the first key within the valid range.
 *
 *  'find' positions at the first key equal or greater than a given key
 *  (prefix).
 *
 *      key:kInclusive       = key
 *      key:kExclusiveBefore = key:1
 *      key:kExclusiveAfter  = key:254
 *
 *
 *  seek( 'a3', exclusive ), forward
 *  =>
 *     a1 a2 a2 a2 a3 a3 a3 a3 a4 a4 a5 a6 a6 a6 a6 a7 a8 a8 a8 a8
 *                            ^__ startKey( 'a3':kExclusiveAfter )
 *                             ^__ nextKey()
 *  or
 *     a1 a2 a2 a2 a3 a3 a3 a3 rightEOF
 *                            ^__ startKey( 'a3':kExclusiveAfter )
 *                              ^__ nextKey()
 *
 *
 *  seek( 'a3', inclusive), forward
 *  =>
 *     a1 a2 a2 a2 a3 a3 a3 a3 a4 a4 a5 a6 a6 a6 a6 a7 a8 a8 a8 a8
 *                ^__ startKey( 'a3' )
 *                 ^__ nextKey()
 *
 *  or [key gap]
 *     a1 a2 a2 a2 a4 a4 a5 a6 a6 a6 a6 a7 a8 a8 a8 a8
 *                ^__ startKey( 'a3' )
 *                 ^__ nextKey()
 *
 *  or [compound key]
 *     a1:b1 a2:null a2:b2 a2:b3 a3:b4 a3:b5 a3:b6 a4:b6 a4:b7
 *          ^__ startKey( 'a2:minKey' )
 *                   ^__ nextKey()
 *
 *
 *
 *  seek( 'a3', false ), backwards, exclusive
 *  =>
 *     a1 a2 a2 a2 a3 a3 a3 a3 a4 a4 a5 a6 a6 a6 a6 a7 a8 a8 a8 a8
 *                ^__ startKey( 'a3' )
 *              ^__ prevKey()
 *  or
 *     a1 a2 a2 a2 a4 a4 a5 a6 a6 a6 a6 a7 a8 a8 a8 a8
 *                ^__ startKey( 'a3' )
 *              ^__ prevKey()
 *  or
 *     leftEOF a3 a3 a3 a3 a4 a4 a5 a6 a6 a6 a6 a7 a8 a8 a8 a8
 *            ^__ startKey( 'a3' )
 *          ^__ prevKey()
 *
 *
 *  seek( 'a3', true ), backwards, inclusive
 *  =>
 *     a1 a2 a2 a2 a3 a3 a3 a3 a4 a4 a5 a6 a6 a6 a6 a7 a8 a8 a8 a8
 *                            ^__ startKey( 'a3':kExclusiveAfter )
 *                          ^__ prevKey()
 *  or
 *     a1 a2 a2 a2 a3 a3 a3 a3 rightEOF
 *                            ^__ startKey( 'a3':kExclusiveAfter )
 *                          ^__ prevKey()
 *
 * Question: unique values extracted from array key fields?
 */
boost::optional<IndexKeyEntry> ARTCursor::seek(const BSONObj& bsonKey0,
                                               bool inclusive,
                                               RequestedInfo parts) {
    BSONObj bsonKey = ARTreeSortedDataImpl::stripFieldNames(bsonKey0);

    if (FUNCTION_TRACE)
        log() << "ARTCursor::" << __FUNCTION__ << ":" << __LINE__
              << " key = " << bsonKey.toString(0, 0);

    KeyString::Discriminator disc;
    disc = (_isForward == inclusive) ? KeyString::kInclusive : KeyString::kExclusiveAfter;
    KeyString key(bsonKey, _ordering, disc);

    _artreeCursor->startDocKey(
        reinterpret_cast<const uint8_t*>(key.getBuffer()), key.getSize(), _isForward, inclusive);

    if (_artreeCursor->atEOF() || _artreeCursor->isDone())
        return {};

    switch (parts) {
        case kJustExistance:
            return IndexKeyEntry(BSONObj(), RecordId());
        case kWantKey:
            return IndexKeyEntry(getKey(), RecordId());
        case kWantLoc:
            return IndexKeyEntry(BSONObj(), getRecordId());
        case kKeyAndLoc:
            return IndexKeyEntry(getKey(), getRecordId());
        default:
            ;
    }

    return {};
}

/**
 *  Moves forward and returns the new data or boost::none if there is no more
 * data.
 *  If not positioned, returns boost::none.
 */
boost::optional<IndexKeyEntry> ARTCursor::next(RequestedInfo parts) {
    if (FUNCTION_TRACE)
        log() << "ARTCursor::" << __FUNCTION__;

    if (_artreeCursor->atEOF() || _artreeCursor->_doneBit)
        return {};

    if (_isForward)
        _artreeCursor->nextKey(_artreeCursor->_queue->_value, _isForward);
    else
        _artreeCursor->prevKey(_artreeCursor->_queue->_value, _isForward);

    if (_artreeCursor->_doneBit)
        return {};

    switch (parts) {
        case kJustExistance:
            return IndexKeyEntry(BSONObj(), RecordId());
        case kWantKey:
            return IndexKeyEntry(getKey(), RecordId());
        case kWantLoc:
            return IndexKeyEntry(BSONObj(), getRecordId());
        case kKeyAndLoc:
            return IndexKeyEntry(getKey(), getRecordId());
        default:
            MONGO_UNREACHABLE;
            ;
    }

    return {};
}

/**
 *  Seeks to the position described by seekPoint and returns the current
 * position.
 */
boost::optional<IndexKeyEntry> ARTCursor::seek(const IndexSeekPoint& seekPoint,
                                               RequestedInfo parts) {
    BSONObj bsonKey = IndexEntryComparison::makeQueryObject(seekPoint, _isForward);
    bool inclusive = !seekPoint.prefixExclusive;

    int i = 0;
    if (inclusive) {
        for (const auto& b : seekPoint.suffixInclusive) {
            if (i++ < seekPoint.prefixLen)
                continue;
            if (!b) {
                inclusive = false;
                break;
            }
        }
    }

    return seek(bsonKey, inclusive, parts);
}

/**
 * Prepares for state changes in underlying data in a way that allows the
 * cursor's
 * current position to be restored.
 */
void ARTCursor::save() {
    if (FUNCTION_TRACE)
        log() << "ARTCursor::" << __FUNCTION__ << ":" << __LINE__;

    _artreeCursor->saveCursor();
    return;
}

/**
 * Recovers from potential state changes in underlying data.
 */
void ARTCursor::restore() {
    if (FUNCTION_TRACE)
        log() << "ARTCursor::" << __FUNCTION__ << ":" << __LINE__;

    _artreeCursor->restoreCursor(_isForward);
}

void ARTCursor::detachFromOperationContext() {
    if (FUNCTION_TRACE)
        log() << "ARTCursor::" << __FUNCTION__ << ":" << __LINE__;

    // unlock all storage engine resources
}

void ARTCursor::reattachToOperationContext(OperationContext* opCtx) {
    if (FUNCTION_TRACE)
        log() << "ARTCursor::" << __FUNCTION__ << ":" << __LINE__;

    // the model is that you are in effect creating a new cursor
    // and seeking to the last position.
    ARTreeRecoveryUnit* recUnit = ARTree::recUnit(opCtx);
    recUnit->resetTxnId();
    _opCtx = opCtx;
}

}  // namespace mongo
