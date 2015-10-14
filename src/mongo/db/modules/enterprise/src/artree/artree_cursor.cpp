
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

#include "mongo/platform/basic.h"
#include "artree_cursor.h"

#include <cstdlib>

#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

#include "artree.h"
#include "artree_common.h"
#include "artree_cursor.h"
#include "artree_debug.h"
#include "artree_index.h"
#include "artree_nodes.h"
#include "artree_records.h"
#include "artree_util.h"

/*
namespace {
static const uint8_t kMinKey = 0x00;
static const uint8_t kMaxKey = 0xff;
}
*/

namespace mongo {

/**
 *  make a new cursor
 */
ARTreeCursor* ARTreeCursor::newCursor(void* opCtx, ARTreeIndex* index, uint32_t stackMax) {
    ARTreeCursor* cursor = static_cast<ARTreeCursor*>(
        calloc(1, sizeof(ARTreeCursor) + stackMax * sizeof(ARTreeStack)));

    cursor->_maxDepth = MIN_cursor + stackMax;  // maximum depth of ART
    cursor->_art = index->_art;                 // ARTree arena
    cursor->_index = index;                     // the index we are scanning
    cursor->_keySize = 0;                       // size of key incl recno
    cursor->_keyXtra = 0;                       // size of recno in key
    cursor->_endKeySize = 0;                    // size of end key undefined
    cursor->_depth = 0;                         // current depth
    cursor->_atEOF = false;                     // true;
    cursor->_atLeftEOF = false;                 // true;
    cursor->_atRightEOF = false;                // true;
    cursor->_restoreNotExact = false;
    cursor->_restoreNotUsed = false;
    cursor->_save = nullptr;
    cursor->_opCtx = opCtx;
    memset(cursor->_queue, 0, sizeof(cursor->_queue));
    cursor->_art->enqueueCursor(cursor->_queue);
    return cursor;
}

void ARTreeCursor::resetCursor() {
    // move cursor to the front of the queue
    if (_queue->_value)
        _art->dequeueCursor(_queue);
    _art->enqueueCursor(_queue);

    _depth = 0;
    _keySize = 0;
    _doneBit = false;
    _atLeftEOF = false;
    _atRightEOF = false;
    _atEOF = false;
    _restoreNotExact = false;
}

void ARTreeCursor::endCursor(ARTreeCursor* cursor) {
    if (cursor->_queue->_value)
        cursor->_art->dequeueCursor(cursor->_queue);
    if (cursor->_save)
        free(cursor->_save);
    free(cursor);
}

void ARTreeCursor::saveCursor() {
    if (_restoreNotUsed)
        return;
    if (_save)
        free(_save);

    ARTreeCursor* save = static_cast<ARTreeCursor*>(calloc(1, sizeof(ARTreeCursor)));

    memcpy(save, this, sizeof(ARTreeCursor));
    _save = save;

    if (CURSOR_TRACE)
        log() << "ARTreeCursor::" << __FUNCTION__ << ":" << __LINE__ << " : "
              << "_save = " << (uint64_t)_save;
}

void ARTreeCursor::restoreCursor(bool isForward) {
    // move cursor to the front of the queue
    if (_queue->_value)
        _art->dequeueCursor(_queue);
    _art->enqueueCursor(_queue);

    if (_save->_atEOF) {
        _atEOF = true;
        _atLeftEOF = _save->_atLeftEOF;
        _atRightEOF = _save->_atRightEOF;
        _restoreNotExact = false;
    } else {
        _restoreNotExact = !findDocKey(_save->_key, _save->_keySize, isForward);
    }

    _restoreNotUsed = true;
}

/**
 * note: used by either 4 or 14 slot node
 * returns entry previous to 'ch'
 * algorithm: place each key byte into radix array, scan backwards
 *              from 'ch' to preceeding entry.
 */
int ARTreeCursor::slotrev4x14(int ch, uint8_t max, uint32_t alloc, volatile uint8_t* keys) {
    uint8_t radix[256];
    memset(radix, 0xff, sizeof(radix));

    for (uint32_t slot = 0; slot < max; slot++) {
        if (alloc & (1 << slot))
            radix[keys[slot]] = slot;
    }

    while (--ch >= 0) {
        if (radix[ch] < 0xff)
            return radix[ch];
    }
    return -1;
}

int ARTreeCursor::slot4x14(int ch, uint8_t max, uint32_t alloc, volatile uint8_t* keys) {
    uint8_t radix[256];
    memset(radix, 0xff, sizeof(radix));

    for (uint32_t slot = 0; slot < max; slot++) {
        if (alloc & (1 << slot))
            radix[keys[slot]] = slot;
    }

    while (++ch < 256) {
        invariant(ch >= 0);
        if (radix[ch] < 0xff)
            return radix[ch];
    }
    return 256;
}

int ARTreeCursor::slotrev64(int ch, uint64_t alloc, volatile uint8_t* keys) {
    while (--ch >= 0) {
        if (keys[ch] < 0xff)
            if (alloc & (1ULL << keys[ch]))
                return ch;
    }
    return -1;
}

int ARTreeCursor::slot64(int ch, uint64_t alloc, volatile uint8_t* keys) {
    while (++ch < 256) {
        invariant(ch >= 0);
        if (keys[ch] < 0xff)
            if (alloc & (1ULL << keys[ch]))
                return ch;
    }
    return 256;
}

/**
 * extract the RecordId from index bytes
 * note: this method is slated for replacement
 */
uint32_t ARTreeCursor::extractRecId(const uint8_t* key, uint32_t keylen, ARTAddr* recId) {
    uint32_t xtrabytes = key[keylen - 1] & 0x7;
    recId->off = key[keylen - 1] >> 3;
    for (uint32_t idx = 0; idx < xtrabytes; idx++) {
        recId->off |= (uint64_t)key[keylen - idx - 2] << (idx * 8 + 5);
    }
    recId->off |= (((uint64_t)key[keylen - xtrabytes - 2] & 0x1f) << (xtrabytes * 8 + 5));
    return xtrabytes;
}

/**
 * find closest available doc (during restore)
 * returns true on exact match only
 */
bool ARTreeCursor::findDocKey(const uint8_t* key, uint32_t keylen, bool isForward) {
    ARTSlot* slot = startKey(key, keylen);

    if (_depth && slot->type != EndKey) {
        ARTreeStack* stack = &_stack[_depth - 1];
        if (stack->ch >= 0)
            stack->ch--;  // prepare for nextKey(), which will first
                          // increment 'ch' on it's way to the first
                          // record id that matches the key.
    }

    // following logic is required for correct operation of reverse
    // cursors when no previous record is available.

    nextKey(_queue->_value, isForward);

    if (!isForward)
        prevKey(_queue->_value, isForward);

    while (!isDone()) {
        int s = memcmp(_key, key, std::min(_keySize, keylen));
        if (!s)
            s = (_keySize - keylen);
        if (isForward && s > 0)
            break;
        if (!isForward && s < 0)
            break;

        // identical : done
        if (!s)
            return true;

        if (isForward)
            nextKey(_queue->_value, isForward);
        else
            prevKey(_queue->_value, isForward);
    }

    return false;
}

void ARTreeCursor::startDocKey(const uint8_t* key,
                               uint32_t keylen,
                               bool isForward,
                               bool inclusive) {
    if (CURSOR_TRACE) {
        log() << "ENTER " << __FUNCTION__ << ":" << __LINE__ << "( " << ARTree::keyStr(key, keylen)
              << ", isForward = " << isForward << ", inclusive = " << inclusive << " )";
        _art->printIndex(_index);
    }

    ARTSlot* slot = startKey(key, keylen);

    if (_depth) {
        ARTreeStack* stack = &_stack[_depth - 1];
        if (isForward) {
            if (stack->ch >= 0)
                stack->ch--;
        }
    }

    if (CURSOR_TRACE) {
        log() << "FOLLOWING 'startKey':\n";
        _art->printCursor(this);
        if (slot)
            log() << "\n\n  slot = " << _art->slotStr(slot, true) << '\n' << '\n';
    }

    if (isForward)
        nextKey(_queue->_value, isForward);
    else
        prevKey(_queue->_value, isForward);

    if (CURSOR_TRACE) {
        log() << "ON EXIT:\n";
        _art->printCursor(this);
    }
}

/**
 * position cursor at or after requested key
 */
ARTSlot* ARTreeCursor::startKey(const uint8_t* key, uint32_t keylen) {
    resetCursor();
    return _index->findKey(this, _index->_root, key, keylen);
}

/**
 * retrieve next key from cursor
 * note:
 *   nextKey sets EOF when it cannot advance and isForward==true
 *   prevKey sets EOF when it cannot advance and isForward==false
 *
 * TODO: check to see if we can remove the 'isForward' parameter that
 *       appears in 'nextKey' and 'prevKey'.
 */
bool ARTreeCursor::nextKey(uint64_t txnId, bool isForward) {
    if (_restoreNotUsed)
        _restoreNotUsed = false;

    if (_restoreNotExact) {
        _restoreNotExact = false;
        return !_atEOF;
    }

    // follow version chain to visible record
    while (nextKey(isForward)) {
        if (!_art->_arena[0]->_arenaRec.off)  // sorted_data_impl_test?
            break;
        if (_art->isVisibleVersion(_opCtx, txnId, _recordId))
            break;
    }

    if (_atEOF) {
        _doneBit = true;
        return false;
    }

    if (CURSOR_TRACE) {
        log() << "\n\nARTreeCursor::" << __FUNCTION__ << ":" << __LINE__ << "( " << txnId << " )"
              << "\n  key        = " << ARTree::keyStr(_key, _keySize)
              << "\n  endKey     = " << ARTree::keyStr(_endKey, _endKeySize)
              << "\n  keySize    = " << _keySize << "\n  endKeySize = " << _endKeySize << '\n';
    }

    if (!_endKeySize) {
        _doneBit = false;
        return true;
    }

    int s = memcmp(_key, _endKey, std::min(_keySize - _keyXtra, _endKeySize));
    if (!s)
        s = (_keySize - _keyXtra - _endKeySize);

    bool notDone;
    if (!s)
        notDone = _endKeyInclusive;
    else
        notDone = (isForward ? s < 0 : s > 0);

    _doneBit = !notDone;
    return notDone;
}

bool ARTreeCursor::nextKey(bool isForward) {
    if (_atRightEOF)
        return false;

    if (!_depth || _atLeftEOF) {
        _depth = 0;
        ARTreeStack* stack = &_stack[_depth++];
        stack->slot->bits = _index->_root->bits;
        stack->addr = _index->_root;
        stack->off = 0;
        stack->ch = -1;
        _atLeftEOF = false;
        _atEOF = false;
    }

    while (_depth < _maxDepth) {
        ARTreeStack* stack = &_stack[_depth - 1];
        _keySize = stack->off;

        switch (stack->slot->type) {
            case EndKey: {
                uint32_t xtrabytes = extractRecId(_key, _keySize, &_recordId);
                _versionedId = _recordId;

                // retrieve value for the key

                if (stack->slot->off) {
                    _value = _art->arenaSlotAddr(stack->slot);
                } else {
                    _value = nullptr;
                }

                _keyXtra = xtrabytes + 2;  // total key byte length includes
                                           // two 5-bit sentinels at either end
                _depth--;
                return true;
            }

            case SpanNode: {
                ARTspan* spanNode = static_cast<ARTspan*>(_art->arenaSlotAddr(stack->slot));

                uint32_t max = stack->slot->nbyte;

                //  continue into our next slot

                if (stack->ch < 0) {
                    memcpy(_key + _keySize, spanNode->bytes, max);
                    _keySize += max;
                    _stack[_depth].slot->bits = spanNode->next->bits;
                    _stack[_depth].addr = spanNode->next;
                    _stack[_depth].ch = -1;
                    _stack[_depth++].off = _keySize;
                    stack->ch = 0;
                    continue;
                }

                break;
            }

            case Array4: {
                ARTnode4* radix4Node = static_cast<ARTnode4*>(_art->arenaSlotAddr(stack->slot));

                uint32_t slot = slot4x14(stack->ch, 4, radix4Node->alloc, radix4Node->keys);
                if (slot >= 4)
                    break;

                stack->ch = radix4Node->keys[slot];
                _key[_keySize++] = radix4Node->keys[slot];

                _stack[_depth].slot->bits = radix4Node->radix[slot].bits;
                _stack[_depth].addr = &radix4Node->radix[slot];
                _stack[_depth].off = _keySize;
                _stack[_depth++].ch = -1;
                continue;
            }

            case Array14: {
                ARTnode14* radix14Node = static_cast<ARTnode14*>(_art->arenaSlotAddr(stack->slot));

                uint32_t slot = slot4x14(stack->ch, 14, radix14Node->alloc, radix14Node->keys);
                if (slot >= 14)
                    break;

                stack->ch = radix14Node->keys[slot];
                _key[_keySize++] = radix14Node->keys[slot];
                _stack[_depth].slot->bits = radix14Node->radix[slot].bits;
                _stack[_depth].addr = &radix14Node->radix[slot];
                _stack[_depth].ch = -1;
                _stack[_depth++].off = _keySize;
                continue;
            }

            case Array64: {
                ARTnode64* radix64Node = static_cast<ARTnode64*>(_art->arenaSlotAddr(stack->slot));

                stack->ch = slot64(stack->ch, radix64Node->alloc, radix64Node->keys);
                if (stack->ch == 256)
                    break;

                _key[_keySize++] = stack->ch;
                _stack[_depth].slot->bits = radix64Node->radix[radix64Node->keys[stack->ch]].bits;
                _stack[_depth].addr = &radix64Node->radix[radix64Node->keys[stack->ch]];
                _stack[_depth].ch = -1;
                _stack[_depth++].off = _keySize;
                continue;
            }

            case Array256: {
                ARTnode256* radix256Node =
                    static_cast<ARTnode256*>(_art->arenaSlotAddr(stack->slot));

                while (stack->ch < 256) {
                    uint32_t idx = ++stack->ch;
                    if (radix256Node->alloc[idx / 64] & (1ULL << (idx % 64)))
                        break;
                }

                if (stack->ch == 256)
                    break;

                _key[_keySize++] = stack->ch;
                _stack[_depth].slot->bits = radix256Node->radix[stack->ch].bits;
                _stack[_depth].addr = &radix256Node->radix[stack->ch];
                _stack[_depth].ch = -1;
                _stack[_depth++].off = _keySize;
                continue;
            }
        }  // end switch

        if (--_depth) {
            stack = &_stack[_depth];
            _keySize = stack->off;
            //_keySize = stack[-1].off;
            continue;
        }

        _atRightEOF = true;
        _atEOF = isForward;
        _recordId.off = 0;
        return false;

    }  // end while

    MONGO_UNREACHABLE;
}

/**
 * retrieve previous key from the cursor
 */
bool ARTreeCursor::prevKey(uint64_t txnId, bool isForward) {
    if (_restoreNotUsed)
        _restoreNotUsed = false;

    if (_restoreNotExact) {
        _restoreNotExact = false;
        return !_atEOF;
    }

    // follow version chain to visible records
    while (prevKey(isForward)) {
        if (!_art->_arena[0]->_arenaRec.off)  // sorted_data_impl_test?
            break;
        if (_art->isVisibleVersion(_opCtx, txnId, _recordId))
            break;
    }
    // prevKey(isForward);

    if (_atEOF) {
        _doneBit = true;
        return false;
    }

    if (CURSOR_TRACE) {
        log() << "\n\nARTreeCursor::" << __FUNCTION__ << ":" << __LINE__ << "( " << txnId << " )"
              << "\n  key        = " << ARTree::keyStr(_key, _keySize)
              << "\n  endKey     = " << ARTree::keyStr(_endKey, _endKeySize)
              << "\n  keySize    = " << _keySize << "\n  endKeySize = " << _endKeySize << '\n';
    }

    if (!_endKeySize) {
        _doneBit = false;
        return true;
    }

    int s = memcmp(_key, _endKey, std::min(_keySize - _keyXtra, _endKeySize));
    if (!s)
        s = (_keySize - _keyXtra - _endKeySize);

    bool notDone;
    if (!s)
        notDone = _endKeyInclusive;
    else
        notDone = (isForward ? s < 0 : s > 0);

    _doneBit = !notDone;
    return notDone;
}

bool ARTreeCursor::prevKey(bool isForward) {
    if (_atLeftEOF)
        return false;

    if (!_depth || _atRightEOF) {
        _depth = 0;
        ARTreeStack* stack = &_stack[_depth++];
        stack->slot->bits = _index->_root->bits;
        stack->addr = _index->_root;
        stack->off = 0;
        stack->ch = 256;
        _atRightEOF = false;
        _atEOF = false;
    }

    while (_depth) {
        ARTreeStack* stack = &_stack[_depth - 1];
        _keySize = stack->off;

        switch (stack->slot->type) {
            case UnusedSlot: {
                break;
            }

            case EndKey: {
                uint32_t xtrabytes = extractRecId(_key, _keySize, &_recordId);
                _versionedId = _recordId;

                // retrieve value for the key
                //
                if (stack->slot->off) {
                    _value = _art->arenaSlotAddr(stack->slot);
                } else {
                    _value = nullptr;
                }

                _keyXtra = xtrabytes + 2;
                _depth--;
                return true;
            }

            case SpanNode: {
                ARTspan* spanNode = static_cast<ARTspan*>(_art->arenaSlotAddr(stack->slot));

                uint32_t max = stack->slot->nbyte;

                // examine next node under slot

                if (stack->ch > 255) {
                    memcpy(_key + _keySize, spanNode->bytes, max);
                    _keySize += max;
                    _stack[_depth].slot->bits = spanNode->next->bits;
                    _stack[_depth].addr = spanNode->next;
                    _stack[_depth].ch = 256;
                    _stack[_depth++].off = _keySize;
                    stack->ch = 0;
                    continue;
                }
                break;
            }

            case Array4: {
                ARTnode4* radix4Node = static_cast<ARTnode4*>(_art->arenaSlotAddr(stack->slot));

                int slot = slotrev4x14(stack->ch, 4, radix4Node->alloc, radix4Node->keys);
                if (slot < 0) {
                    break;
                }

                stack->ch = radix4Node->keys[slot];
                _key[_keySize++] = stack->ch;

                _stack[_depth].slot->bits = radix4Node->radix[slot].bits;
                _stack[_depth].addr = &radix4Node->radix[slot];
                _stack[_depth].off = _keySize;
                _stack[_depth++].ch = 256;
                continue;
            }

            case Array14: {
                ARTnode14* radix14Node = static_cast<ARTnode14*>(_art->arenaSlotAddr(stack->slot));

                int slot = slotrev4x14(stack->ch, 14, radix14Node->alloc, radix14Node->keys);
                if (slot < 0)
                    break;

                stack->ch = radix14Node->keys[slot];
                _key[_keySize++] = stack->ch;

                _stack[_depth].slot->bits = radix14Node->radix[slot].bits;
                _stack[_depth].addr = &radix14Node->radix[slot];
                _stack[_depth].off = _keySize;
                _stack[_depth++].ch = 256;
                continue;
            }

            case Array64: {
                ARTnode64* radix64Node = static_cast<ARTnode64*>(_art->arenaSlotAddr(stack->slot));

                stack->ch = slotrev64(stack->ch, radix64Node->alloc, radix64Node->keys);
                if (stack->ch < 0)
                    break;

                uint32_t slot = radix64Node->keys[stack->ch];
                _key[_keySize++] = stack->ch;

                _stack[_depth].slot->bits = radix64Node->radix[slot].bits;
                _stack[_depth].addr = &radix64Node->radix[slot];
                _stack[_depth].off = _keySize;
                _stack[_depth++].ch = 256;
                continue;
            }

            case Array256: {
                ARTnode256* radix256Node =
                    static_cast<ARTnode256*>(_art->arenaSlotAddr(stack->slot));

                while (--stack->ch >= 0) {
                    uint32_t idx = stack->ch;
                    if (radix256Node->alloc[idx / 64] & (1ULL << (idx % 64)))
                        break;
                }

                if (stack->ch < 0)
                    break;

                uint32_t slot = stack->ch;
                _key[_keySize++] = stack->ch;

                _stack[_depth].slot->bits = radix256Node->radix[slot].bits;
                _stack[_depth].addr = &radix256Node->radix[slot];
                _stack[_depth].off = _keySize;
                _stack[_depth++].ch = 256;
                continue;
            }
        }  // end switch

        if (--_depth) {
            _keySize = stack[-1].off;
            continue;
        }

        break;
    }  // end while

    _atEOF = !isForward;
    _atLeftEOF = true;
    _recordId.off = 0;
    return false;
}

}  // namespace mongo
