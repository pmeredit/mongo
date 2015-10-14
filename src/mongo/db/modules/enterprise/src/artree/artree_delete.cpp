//@file artree_delete.cpp
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

#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/util/log.h"

#include "artree_cursor.h"
#include "artree_debug.h"
#include "artree_index.h"
#include "artree_nodes.h"
#include "artree_util.h"

#include <cstdlib>
#include <cstring>

namespace mongo {

//
// radix tree delete Key
//    return false if key not found
//
bool ARTreeIndex::deleteDocKey(
    uint32_t set, const uint8_t* key, uint32_t keylen, uint64_t recId, ARTSlot* docSlot) {
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
    return deleteDocKey(set, NULL, combo, keylen + xtrabytes, docSlot);
}

//
// radix tree delete Key
//    return false if key not found
//
bool ARTreeIndex::deleteDocKey(
    uint32_t set, void* opCtx, const uint8_t* key, uint32_t keylen, ARTSlot* docSlot) {
    if (FUNCTION_TRACE)
        log() << "ARTreeIndex::" << __FUNCTION__ << ":" << __LINE__;

    ARTreeCursor* cursor = ARTreeCursor::newCursor(opCtx, this, artree::maxkey);
    ARTSlot* endSlot = findKey(cursor, _root, key, keylen);

    //  was key not found in the trie?
    if (!endSlot || endSlot->type != EndKey) {
        ARTreeCursor::endCursor(cursor);
        return false;
    }

    if (docSlot)
        docSlot->bits = endSlot->bits;

    //  now that we have the trie nodes in the cursor stack
    //    we can go through them backwards to remove empties.

    ParamStruct p[1] = {0};
    p->cursor = cursor;
    p->slot = endSlot;
    p->art = _art;
    p->set = set;

    ReturnState rt = EndSearch;

    while (cursor->_depth) {
        ARTreeStack* stack = &cursor->_stack[--cursor->_depth];
        p->prev = p->slot;
        p->slot = stack->addr;
        p->ch = stack->ch;
        uint32_t pass = 0;

        //  wait if we run into a dead slot

        bool retry = true;
        do {
            if (pass)
                ARTreeYield();
            else
                pass = 1;

            // obtain write lock on the node
            MutexSpinLock::lock(p->slot);
            p->newSlot->bits = stack->addr->bits;

            if ((retry = p->newSlot->dead)) {
                MutexSpinLock::unlock(p->slot);
            }

        } while (retry);

        switch (p->newSlot->type) {
            case UnusedSlot: {
                rt = EndSearch;
                break;
            }

            case SpanNode: {
                ARTspan* spanNode = reinterpret_cast<ARTspan*>(_art->arenaSlotAddr(stack->addr));
                rt = spanNode->deleteKey(p);
                break;
            }

            case Array4: {
                ARTnode4* radix4Node =
                    reinterpret_cast<ARTnode4*>(_art->arenaSlotAddr(stack->addr));
                rt = radix4Node->deleteKey(p);
                break;
            }

            case Array14: {
                ARTnode14* radix14Node =
                    reinterpret_cast<ARTnode14*>(_art->arenaSlotAddr(stack->addr));
                rt = radix14Node->deleteKey(p);
                break;
            }

            case Array64: {
                ARTnode64* radix64Node =
                    reinterpret_cast<ARTnode64*>(_art->arenaSlotAddr(stack->addr));
                rt = radix64Node->deleteKey(p);
                break;
            }

            case Array256: {
                ARTnode256* radix256Node =
                    reinterpret_cast<ARTnode256*>(_art->arenaSlotAddr(stack->addr));
                rt = radix256Node->deleteKey(p);
                break;
            }

            case EndKey: {
                //    process endkey slot type bits
                if (p->slot->nbits > 2)
                    _art->arenaFreeStorage(set, p->slot);

                p->slot->dead = 1;
                rt = ContinueSearch;
                break;
            }
        }  // end switch

        MutexSpinLock::unlock(stack->addr);

        if (rt == ContinueSearch)
            continue;

        break;

    }  // end while

    //  zero out root?
    if (!cursor->_depth && rt == ContinueSearch)
        p->slot->bits = 0;

    ARTreeCursor::endCursor(cursor);
    _numEntries--;

    return true;
}

}  // namespace mongo
