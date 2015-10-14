//@file artree_frame.cpp
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

#include "mongo/platform/basic.h"
#include "artree_frame.h"

#include <iostream>
#include <limits>
#include <cstdlib>

#include "artree.h"
#include "artree_nodes.h"
#include "artree_ordered_list.h"
#include "artree_records.h"
#include "artree_util.h"

namespace mongo {

//
//    return frame address from slot entry
//
ARTFrame* ARTree::arenaFrameAddr(ARTSlot* slot) const {
    return static_cast<ARTFrame*>(arenaSlotAddr(slot->addr));
}

//
//    obtain available frame
//
ARTAddr ARTree::arenaAllocFrame() {
    if (!_freeFrame->off)
        return arenaAlloc(sizeof(ARTFrame));

    MutexSpinLock::lock(_freeFrame);

    if (!_freeFrame->off) {
        MutexSpinLock::unlock(_freeFrame);
        return arenaAlloc(sizeof(ARTFrame));
    }

    ARTSlot slot[1];
    slot->bits = _freeFrame->bits;

    ARTFrame* frame = arenaFrameAddr(slot);
    _freeFrame->bits = frame->_next->bits;  // install off & mutex
    frame->_next->bits = 0;                 // this will clear the mutex bit
    frame->_prev->bits = 0;
    return slot->addr;
}

//
//  Add slot to cursor free frame
//
void ARTree::addSlotToFrame(ARTSlot* head, ARTSlot* slot) {
    MutexSpinLock::lock(head);
    ARTFrame* frame = arenaFrameAddr(head);

    if (head->nslot < FrameSlots) {
        frame->_slots[head->nslot++].bits = slot->bits;
        MutexSpinLock::unlock(head);
        return;
    }

    //  otherwise add slot to new free
    //    frame, and put at head of free/wait queue

    ARTSlot slot2[1];
    slot2->bits = 0;

    slot2->addr = arenaAllocFrame();
    ARTFrame* frame2 = arenaFrameAddr(slot2);

    frame2->_slots->bits = slot->bits;  // install in slot one
    frame2->_next->bits = head->bits;

    // install new head
    //    with lock cleared

    ARTSlot wait2[1];
    wait2->bits = slot2->bits;
    wait2->nslot = 1;

    head->bits = wait2->bits;
}

//
//  Add slot to cursor free/wait queue frame
//
void ARTree::addSlotToFrame(ARTSlot* head, ARTSlot* tail, ARTSlot* slot) {
    MutexSpinLock::lock(head);

    //  add slot to existing wait frame?

    ARTFrame* frame = arenaFrameAddr(head);

    if (head->off)
        if (head->nslot < FrameSlots) {
            frame->_slots[head->nslot++].bits = slot->bits;
            frame->_timestamp = _engine->allocTxnId(_engine->en_writer);
            MutexSpinLock::unlock(head);
            return;
        }

    //  otherwise add slot to new wait
    //    frame, and put at head of free/wait queue

    ARTSlot slot2[1];
    slot2->bits = 0;

    slot2->addr = arenaAllocFrame();
    ARTFrame* frame2 = arenaFrameAddr(slot2);

    frame2->_slots->bits = slot->bits;  // install in slot one
    frame2->_timestamp = _engine->allocTxnId(_engine->en_writer);
    frame2->_next->bits = head->bits;

    if (tail)
        if (head->off) {
            frame->_prev->bits = slot2->bits;
            if (!frame->_next->off) {
                ARTSlot wait3[1];
                wait3->bits = head->bits;
                wait3->mutex = 0;
                tail->bits = wait3->bits;
            }
        }

    // install new head of wait queue
    //    with lock cleared

    ARTSlot wait2[1];
    wait2->bits = 0;
    wait2->off = slot2->off;
    wait2->nslot = 1;

    head->bits = wait2->bits;
}

//  add empty frame to free list
//  call with frame address

void ARTree::putFrameOnFreeList(ARTFrame* frame, ARTAddr addr) {
    MutexSpinLock::lock(_freeFrame);
    frame->_next->bits = 0;
    frame->_next->off = _freeFrame->off;
    _freeFrame->off = addr.off;
    MutexSpinLock::unlock(_freeFrame);
}

//
//    Fill in slot with node from free frame
//    call with free slot locked
//
//  always keep one frame attached to free list
//
bool ARTree::getSlotFromFrame(ARTSlot* queue, ARTSlot* slot) {
    //  does the existing frame have any more nodes?

    do {
        ARTAddr addr;
        addr.off = queue->off;
        ARTFrame* frame = arenaFrameAddr(queue);

        if (queue->nslot > 0) {
            slot->bits = 0;
            slot->off = frame->_slots[--queue->nslot].off;
            return true;
        }

        // is there a next frame slot?

        if (!frame->_next->off)
            break;

        ARTSlot next[1];
        next->bits = 0;
        next->mutex = 1;
        next->addr = frame->_next->addr;
        next->nslot = FrameSlots;
        queue->bits = next->bits;
        putFrameOnFreeList(frame, addr);
    } while (1);

    return false;
}

//
//    pull wait queue frame to free list
//
bool ARTree::getFrameFromNodeWait(ARTSlot* queue, ARTSlot* tail) {
    if (!tail->off)
        return false;

    MutexSpinLock::lock(tail);
    ARTFrame* frame2 = arenaFrameAddr(tail);

    uint64_t ts = frame2->_timestamp;
    if (!tail->off || ts >= _cursors->getMinimum() || (_engine ? _engine->isObsolete(ts) : false)) {
        MutexSpinLock::unlock(tail);
        return false;
    }

    // wait time has expired, so we can
    // pull frame from tail of wait queue

    ARTFrame* frame = arenaFrameAddr(queue);
    putFrameOnFreeList(frame, queue->addr);

    queue->nslot = FrameSlots;
    queue->off = tail->off;

    // remove old tail and compute new tail slot

    ARTFrame* frame3 = arenaFrameAddr(frame2->_prev);
    frame3->_next->bits = 0;

    //  is this the last frame in the prev chain?

    if (frame3->_prev->off)
        tail->bits = frame2->_prev->bits;
    else
        tail->bits = 0;

    MutexSpinLock::unlock(tail);
    return true;
}

void ARTree::enqueueCursor(ARTreeOrderedList::Entry* queue) {
    _cursors->insert(queue, _engine->allocTxnId(_engine->en_reader));
}

void ARTree::dequeueCursor(ARTreeOrderedList::Entry* queue) {
    _cursors->remove(queue);
}

}  // namespace mongo
