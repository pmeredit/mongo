//@file mutex_spinlock.cpp
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
#include "artree_util.h"

#ifdef _WIN32
#include <windows.h>
#include <intrin.h>
#else
#include <sched.h>
#include <sys/mman.h>
#include <cxxabi.h>
#include <execinfo.h>
#define _FILE_OFFSET_BITS 64
#endif

#include <stdint.h>
#include <cstdlib>
#include <iostream>
#include <memory>

#include "artree_nodes.h"

namespace mongo {

void MutexSpinLock::lock(volatile char* latch) {
#ifndef _WIN32
    while (__sync_fetch_and_or(latch, 1)) {
#else
    while (_InterlockedOr8(latch, 1)) {
#endif
        do
#ifndef _WIN32
            relax();
#else
            SwitchToThread();
#endif
        while (latch[0]);
    }
}

void MutexSpinLock::lock(ARTSlot* slot) {
#ifndef _WIN32
    while (__sync_fetch_and_or((volatile char*)slot->latch, MUTEX_BIT) & MUTEX_BIT) {
#else
    while (_InterlockedOr8((volatile char*)slot->latch, MUTEX_BIT) & MUTEX_BIT) {
#endif
        do
#ifndef _WIN32
            relax();
#else
            SwitchToThread();
#endif
        while (*slot->latch & MUTEX_BIT);
    }
}

// implement tryLock
bool MutexSpinLock::tryLock(ARTSlot* slot) {
    if (slot->mutex)
        return false;
    MutexSpinLock::lock(slot);
    return true;
}

void MutexSpinLock::kill(ARTSlot* slot) {
    *slot->latch |= DEAD_BIT;
}

void MutexSpinLock::unlock(volatile char* latch) {
    *latch = 0;
}

void MutexSpinLock::unlock(ARTSlot* slot) {
    *slot->latch &= ~MUTEX_BIT;
}

void ARTreeYield() {
#ifndef _WIN32
    sched_yield();
#else
    SwitchToThread();
#endif
}

void* ARTreeAlloc(uint64_t max) {
#ifndef _WIN32
    return mmap(nullptr, max, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);
#else
    return VirtualAlloc(nullptr, max, MEM_COMMIT, PAGE_READWRITE);
#endif
}

void ARTreeFree(void* arena, uint32_t max) {
#ifndef _WIN32
    munmap(arena, max);
#else
    VirtualFree(arena, 0, MEM_RELEASE);
#endif
}

uint64_t ARTreeCompareAndSwap(uint64_t* targetPtr, uint64_t compareVal, uint64_t swapVal) {
#ifndef _WIN32
    return __sync_val_compare_and_swap(targetPtr, compareVal, swapVal);
#else
    return InterlockedCompareExchange64((volatile __int64*)targetPtr, swapVal, compareVal);
#endif
}

}  // namespace mongo
