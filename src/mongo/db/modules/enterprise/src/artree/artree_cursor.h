//@file artree_cursor.h
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

#pragma once

#include "mongo/bson/ordering.h"
#include "mongo/db/record_id.h"
#include "artree.h"

namespace mongo {

class ARTreeCursor;

namespace {
static const uint8_t kMinKey = 0x00;
static const uint8_t kMaxKey = 0xff;
}

/**
 * cursor stack element
 * Note: we process keys one character at a time.
 *       The stack records the sequence of characters we traverse.
 */
class ARTreeStack {
public:
    ARTSlot slot[1];  // slot that points to node
    ARTSlot* addr;    // tree addr of slot
    uint32_t off;     // offset within key
    int ch;           // character of key
};

/**
 * cursor control
 */
class ARTreeCursor {
    static const int MIN_cursor = 12;  // default sizeof CursorStack

public:
    ARTreeCursor(ARTreeIndex* index) {
        _atEOF = false;
        _atLeftEOF = false;
        _atRightEOF = false;
        _doneBit = false;
        _restoreNotExact = false;
        _restoreNotUsed = false;
        _depth = 0;       // current depth
        _keySize = 0;     // size of key incl recno
        _keyXtra = 0;     // size of recno in key
        _endKeySize = 0;  // size of end key undefined
        _endKeyInclusive = false;
        _maxDepth = MIN_cursor;  // maximum depth of cursor
        _art = index->_art;
        _value = nullptr;
        _opCtx = nullptr;
        _index = index;
        _save = nullptr;
        _recordId.off = 0;     // current cursor record id
        _versionedId.off = 0;  // current cursor version id

        memset(_queue, 0, sizeof(_queue));
        _art->enqueueCursor(_queue);
    }

    ~ARTreeCursor() {
        if (_queue->_value)
            _art->dequeueCursor(_queue);
    }

    /**
     *  create a new cursor
     */
    static ARTreeCursor* newCursor(void* opCtx, ARTreeIndex* index, uint32_t stackmax);

    /**
     *  delete cursor
     */
    static void endCursor(ARTreeCursor* cursor);

    /**
     *  find next radix ordinal
     */
    static int slot4x14(int ch, uint8_t max, uint32_t alloc, volatile uint8_t* keys);

    /**
     *  find next radix ordinal
     */
    static int slot64(int ch, uint64_t alloc, volatile uint8_t* keys);

    /**
     *  find prev radix ordinal
     */
    static int slotrev4x14(int ch, uint8_t max, uint32_t alloc, volatile uint8_t* keys);

    /**
     *  find prev radix ordinal
     */
    static int slotrev64(int ch, uint64_t alloc, volatile uint8_t* keys);

    /**
     *  reset cursor
     */
    void resetCursor();

    /**
     *	find nearest document key in the index
     */
    bool findDocKey(const uint8_t* key, uint32_t keylen, bool isForward);

    /**
     *  position cursor at or immediately before or after requested key
     */
    ARTSlot* startKey(const uint8_t* key, uint32_t keylen);
    void startDocKey(const uint8_t* key, uint32_t keylen, bool isForward, bool inclusive);

    /**
     *  retrieve next key from cursor
     */
    bool nextKey(bool isForward);
    bool nextKey(uint64_t txnId, bool isForward);

    /**
     *  retrieve previous key from cursor
     */
    bool prevKey(bool isForward);
    bool prevKey(uint64_t txnId, bool isForward);

    /**
     * @return abbreviated keylen (minus redId bytes)
     */
    uint32_t extractRecId(const uint8_t* key, uint32_t keylen, ARTAddr* recId);

    /**
     * save/restore cursor
     */
    void saveCursor();
    void restoreCursor(bool isForward);

    /**
     * return timestamp from queue
     */
    uint64_t getTimestamp() const {
        return _queue->_value;
    }

    /**
     * handful of minor inline access methods used by ARTreeSortedDataImpl
     */
    uint8_t* getKey() const {
        return const_cast<uint8_t*>(_key);
    }
    uint32_t getKeyLen() const {
        return _keySize;
    }
    ARTAddr getRecordId() const {
        return _recordId;
    }
    ARTAddr getVersionedId() const {
        return _versionedId;
    }
    void* getValue() const {
        return _value;
    }
    bool atEOF() const {
        return _atEOF;
    }
    bool isDone() const {
        return _doneBit;
    }
    void setOpCtx(void* opCtx) {
        _opCtx = opCtx;
    }

    bool _atEOF;                         // needed to support 'atEOF()'
    bool _atLeftEOF;                     // needed to support 'atEOF()'
    bool _atRightEOF;                    // needed to support 'atEOF()'
    bool _doneBit;                       // range exhausted
    bool _restoreNotExact;               // restore advanced to next/prev key
    bool _restoreNotUsed;                // restore not yet used : retain saved key
    uint32_t _depth;                     // current depth of cursor
    uint32_t _keySize;                   // current size of key
    uint32_t _keyXtra;                   // current size of recId in key
    uint32_t _endKeySize;                // size of end key
    bool _endKeyInclusive;               // end key included in range
    uint32_t _maxDepth;                  // maximum depth of cursor
    ARTree* _art;                        // ARTree arena
    void* _value;                        // Value for key
    void* _opCtx;                        // Operation Context for this cursor
    ARTAddr _recordId;                   // current cursor recordId
    ARTAddr _versionedId;                // current cursor recordId version
    ARTreeOrderedList::Entry _queue[1];  // priority queue slot
    ARTreeIndex* _index;                 // cursor index
    ARTreeCursor* _save;                 // pushed save stack
    uint8_t _key[artree::maxkey];        // current cursor key
    uint8_t _endKey[artree::maxkey];     // end key
    ARTreeStack _stack[MIN_cursor];      // cursor stack follows
};

}  // namespace mongo
