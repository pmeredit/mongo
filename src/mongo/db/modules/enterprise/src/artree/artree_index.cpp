//@file artree_index.cpp
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
#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/util/log.h"

#include "artree_cursor.h"
#include "artree_debug.h"
#include "artree_index.h"
#include "artree_util.h"

#include <iostream>
#include <cstdlib>
#include <cstring>

namespace mongo {

//
// radix tree index native methods
//

//    return the LeafNode for the key
//      or the slot where the key deviates
//      from what's in the tree.
//

ARTSlot* ARTreeIndex::findKey(ARTreeCursor* cursor,
                              ARTSlot* root,
                              const uint8_t* key,
                              uint32_t keylen) {
    ParamStruct p[1];
    uint32_t startDepth = 0;
    uint32_t startSize = 0;
    bool pass = false;

    if (cursor) {
        startSize = cursor->_keySize;
        startDepth = cursor->_depth;
    }

    bool restart = true;
    ARTreeStack* stack = nullptr;

    do {
        restart = false;

        p->key = key;
        p->art = _art;
        p->cursor = cursor;
        p->keylen = keylen;
        p->slot = root;
        p->off = 0;

        if (cursor) {
            cursor->_keySize = startSize;
            cursor->_depth = startDepth;
        }

        //  if we are waiting on a dead bit to clear
        if (pass)
            ARTreeYield();
        else
            pass = true;

        // loop through all the key bytes
        stack = nullptr;
        while (p->off < p->keylen) {
            if (p->slot->dead) {
                restart = true;
                break;
            }

            if (cursor) {
                stack = &cursor->_stack[cursor->_depth++];
                stack->slot->bits = p->slot->bits;
                stack->addr = p->slot;
                stack->off = cursor->_keySize;
                stack->ch = key[p->off];
            }

            p->newSlot->bits = p->slot->bits;

            switch (p->slot->type) {
                case Optime: {
                    break;
                }
                case EndKey: {
                    break;
                }
                case SpanNode: {
                    ARTspan* spanNode = reinterpret_cast<ARTspan*>(_art->arenaSlotAddr(p->slot));

                    if (ContinueSearch == spanNode->findKey(p))
                        continue;

                    // key byte not found
                    break;
                }
                case Array4: {
                    ARTnode4* radix4Node =
                        reinterpret_cast<ARTnode4*>(_art->arenaSlotAddr(p->slot));

                    if (ContinueSearch == radix4Node->findKey(p))
                        continue;

                    // key byte not found
                    if (cursor) {
                        uint32_t slot = ARTreeCursor::slot4x14(
                            stack->ch, 4, radix4Node->alloc, radix4Node->keys);
                        if (slot < 4) {
                            stack->ch = radix4Node->keys[slot];
                            break;
                        }
                        stack->ch = 256;
                    }
                    break;
                }
                case Array14: {
                    ARTnode14* radix14Node =
                        reinterpret_cast<ARTnode14*>(_art->arenaSlotAddr(p->slot));

                    if (ContinueSearch == radix14Node->findKey(p))
                        continue;

                    // key byte not found
                    if (cursor) {
                        uint32_t slot = ARTreeCursor::slot4x14(
                            stack->ch, 14, radix14Node->alloc, radix14Node->keys);
                        if (slot < 14) {
                            stack->ch = radix14Node->keys[slot];
                            break;
                        }
                        stack->ch = 256;
                    }
                    break;
                }
                case Array64: {
                    ARTnode64* radix64Node =
                        reinterpret_cast<ARTnode64*>(_art->arenaSlotAddr(p->slot));

                    if (ContinueSearch == radix64Node->findKey(p))
                        continue;

                    // key byte not found
                    if (cursor) {
                        uint32_t slot =
                            ARTreeCursor::slot64(stack->ch, radix64Node->alloc, radix64Node->keys);
                        if (slot < 256) {
                            stack->ch = slot;
                            break;
                        }
                        stack->ch = 256;
                    }
                    break;
                }
                case Array256: {
                    ARTnode256* radix256Node =
                        reinterpret_cast<ARTnode256*>(_art->arenaSlotAddr(p->slot));

                    if (ContinueSearch == radix256Node->findKey(p))
                        continue;

                    // key byte not found
                    if (cursor) {
                        while (stack->ch < 256) {
                            uint32_t bit = ++stack->ch;
                            if (radix256Node->alloc[bit / 64] & (1ULL << (bit % 64)))
                                break;
                        }
                    }
                    break;
                }
                case UnusedSlot: {
                    if (cursor) {
                        cursor->_depth--;
                        if (!cursor->_depth) {
                            cursor->_atEOF = true;
                            cursor->_atRightEOF = true;
                            cursor->_atLeftEOF = true;
                        }
                    }
                    break;
                }
                default:
                    MONGO_UNREACHABLE;

            }  // end switch

            return p->slot;

        }  // end while (p->off < p->keylen)

    } while (restart);

    if (cursor) {
        stack = &cursor->_stack[cursor->_depth++];
        stack->slot->bits = p->slot->bits;
        stack->addr = p->slot;
        stack->off = cursor->_keySize;
        stack->ch = -1;
    }

    return p->slot;
}

bool ARTreeIndex::hasDups(ARTSlot* leaf) {
    if (FUNCTION_TRACE)
        log() << "ARTreeIndex::" << __FUNCTION__ << ":" << __LINE__;

    ARTreeCursor cursor[1] = {ARTreeCursor(this)};

    cursor->_endKeySize = 1;
    cursor->_endKey[0] = 0xff;
    cursor->_atRightEOF = false;
    cursor->_atLeftEOF = false;
    cursor->_atEOF = false;

    ARTreeStack* stack = &cursor->_stack[cursor->_depth++];
    stack->slot->bits = leaf->bits;
    stack->addr = leaf;
    stack->off = 0;
    stack->ch = -1;

    while (cursor->nextKey(true)) {
        if (!_art->isValidRecId(cursor->_recordId.off))
            return true;
        ARTRecord* rec = _art->fetchRec(cursor->_recordId);
        if (!rec)
            return true;  // break;
        if (!rec->_deadstamp)
            return true;
    }

    return false;
}

//
// returns true <=> key is a rejected duplicate
// TODO: remove this method as soon as '_standalone' is decommissioned
//
bool ARTreeIndex::insertDocKey(uint32_t set,
                               const uint8_t* key,
                               uint32_t keylen,
                               bool dupsAllowed,
                               uint64_t recId,
                               ARTSlot* docSlot) {
    uint32_t xtrabits = 16;
    uint8_t combo[artree::maxkey];
    memcpy(combo, key, keylen);
    uint64_t testId = recId >> 10;

    while (testId) {
        testId >>= 1;
        xtrabits++;
    }

    if (uint32_t xtra = xtrabits & 7)
        xtrabits += xtra;

    uint32_t xtrabytes = xtrabits / 8;
    combo[keylen + xtrabytes - 1] = (recId & 0x1f) << 3 | (xtrabytes - 2);
    testId = recId >> 5;

    for (uint32_t idx = xtrabytes - 2; idx; idx--) {
        combo[keylen + idx] = (testId & 0xff);
        testId >>= 8;
    }

    combo[keylen] = testId | ((xtrabytes - 2) << 5);
    return insertDocKey(set, combo, keylen + xtrabytes, dupsAllowed, docSlot);
}

//
// returns true <=> key is a rejected duplicate
//
bool ARTreeIndex::insertDocKey(
    uint32_t set, const uint8_t* key, uint32_t keylen, bool dupsAllowed, ARTSlot* docSlot) {
    if (FUNCTION_TRACE)
        log() << "ARTreeIndex::" << __FUNCTION__ << ":" << __LINE__ << " ==>"
              << "\n  key =" << _art->keyStr(key, keylen) << "\n  dupsAllowed = " << dupsAllowed;

    uint32_t xtrabytes = (key[keylen - 1] & 0x7) + 2;
    ARTSlot* leafSlot;
    ARTSlot* tail;
    do {
        do {
            // note: no change to internal nodes in the case of duplicate keys
            leafSlot = insertKey(_root, set, key, keylen - xtrabytes);
        } while (!leafSlot);

        if (!dupsAllowed && leafSlot->type) {
            if (hasDups(leafSlot)) {
                MutexSpinLock::unlock(leafSlot);
                return true;  // i.e. dupBit = true
            }
        }

        // append record-id part of the key
        tail = insertKey(leafSlot, set, key + keylen - xtrabytes, xtrabytes);

    } while (!tail);

    MutexSpinLock::lock(tail);

    // is this key already in the index with this record-id?
    if (tail->type) {
        MutexSpinLock::unlock(tail);
        return false;
    }

    // install the type-bits
    tail->bits = docSlot->bits;
    _numEntries++;
    return false;
}

ARTSlot* ARTreeIndex::insertKey(ARTSlot* root, uint32_t set, const uint8_t* key, uint32_t keylen) {
    ParamStruct p[1];
    memset(p, 0, sizeof(p));
    bool pass = false;

    bool restart = true;

    do {
        restart = false;

        p->key = key;
        p->art = _art;
        p->cursor = nullptr;
        p->keylen = keylen;
        p->slot = root;
        p->depth = 0;
        p->set = set;
        p->off = 0;

        //  if we are waiting on a dead bit to clear
        if (pass)
            ARTreeYield();
        else
            pass = true;

        while (p->off < p->keylen) {
            p->newSlot->bits = p->slot->bits;
            p->prev = p->slot;
            ReturnState rt = ContinueSearch;

            switch (p->newSlot->type) {
                case SpanNode: {
                    ARTspan* spanNode = reinterpret_cast<ARTspan*>(_art->arenaSlotAddr(p->newSlot));
                    rt = spanNode->insertKey(p);
                    break;
                }

                case Array4: {
                    ARTnode4* radix4Node =
                        reinterpret_cast<ARTnode4*>(_art->arenaSlotAddr(p->newSlot));
                    rt = radix4Node->insertKey(p);
                    break;
                }

                case Array14: {
                    ARTnode14* radix14Node =
                        reinterpret_cast<ARTnode14*>(_art->arenaSlotAddr(p->newSlot));
                    rt = radix14Node->insertKey(p);
                    break;
                }

                case Array64: {
                    ARTnode64* radix64Node =
                        reinterpret_cast<ARTnode64*>(_art->arenaSlotAddr(p->newSlot));
                    rt = radix64Node->insertKey(p);
                    break;
                }

                case Array256: {
                    ARTnode256* radix256Node =
                        reinterpret_cast<ARTnode256*>(_art->arenaSlotAddr(p->newSlot));
                    rt = radix256Node->insertKey(p);
                    break;
                }

                case UnusedSlot: {
                    // obtain write lock on the node
                    MutexSpinLock::lock(p->slot);

                    // restart if slot has been killed
                    // or node has changed.
                    if (p->slot->dead) {
                        MutexSpinLock::unlock(p->slot);
                        rt = RestartSearch;
                        break;
                    }

                    if (p->slot->type) {
                        MutexSpinLock::unlock(p->slot);
                        rt = RetrySearch;
                        break;
                    }

                    p->slot = p->newSlot;
                    p->fillKey();
                    rt = EndSearch;
                    MutexSpinLock::unlock(p->slot);     //@@@ [added 2015.10.12 paul]
                    break;
                }

            }  // end switch

            if (ContinueSearch == rt) {  //  move down the trie one node
                p->depth++;
                continue;
            } else if (RetrySearch == rt)  //  retry the current trie node
                continue;
            else if (RestartSearch == rt) {
                if (!p->depth)
                    return nullptr;
                restart = true;
                break;
            }

            // install and unlock node slot
            ARTSlot slot[1];
            slot->bits = p->prev->bits;

            p->newSlot->mutex = 0;
            p->prev->bits = p->newSlot->bits;

            if (slot->type) {
                if (slot->off != p->newSlot->off) {
                    _art->addSlotToFrame(
                        &_art->_headNode[set][slot->type], &_art->_tailNode[set][slot->type], slot);
                }
            }

            break;

        }  // end while (p->off < p->keylen)

    } while (restart);

    return p->slot;
}

uint64_t ARTreeIndex::numEntries() const {
    return _numEntries.load();
}

Status ARTreeIndex::getMetadata(OperationContext* optCtx, BSONObjBuilder* builder) {
    builder->append("numEntries", static_cast<long long>(numEntries()));
    return Status::OK();
}

}  // namespace mongo
