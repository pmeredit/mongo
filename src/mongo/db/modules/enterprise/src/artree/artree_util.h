//@file mutex_spinlock.h
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
// Some of this is derived code. The original code is in the public domain:
//   ARTful5: Adaptive Radix Trie key-value store
//   Author: Karl Malbrain, malbrain@cal.berkeley.edu
//   Date:   13 JAN 15

#pragma once

#ifdef _WIN32
#include <windows.h>
#include <intrin.h>
#else
#include <sched.h>
#include <sys/mman.h>
#include <cxxabi.h>
#include <execinfo.h>
#endif

#include <memory>

#include "artree_nodes.h"

namespace mongo {

/**
*  one byte mutex spin lock in a slot
*/

#define relax() asm volatile("pause\n" : : : "memory")

class MutexSpinLock {
public:
    static void lock(volatile char* slot);
    static void unlock(volatile char* slot);
    static void lock(ARTSlot* slot);
    static void unlock(ARTSlot* slot);
    static bool tryLock(ARTSlot* slot);
    static void kill(ARTSlot* slot);
};

void ARTreeYield();
void* ARTreeAlloc(uint64_t max);
void ARTreeFree(void* arena, uint32_t max);
uint64_t ARTreeCompareAndSwap(uint64_t* targetPtr, uint64_t compareVal, uint64_t swapVal);

}  // namespace mongo
