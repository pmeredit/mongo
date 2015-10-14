//@file artree.cpp
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
#include "artree.h"

#include "mongo/bson/ordering.h"
#include "mongo/db/jsobj.h"
#include "mongo/util/log.h"

#include "artree_cursor.h"
#include "artree_debug.h"
#include "artree_frame.h"
#include "artree_oplog_store.h"
#include "artree_ordered_list.h"
#include "artree_records.h"
#include "artree_util.h"

#include <iostream>
#include <sstream>
#include <cstdlib>
#include <string>

#ifndef _WIN32
#include <unistd.h>
#include <xmmintrin.h>
#endif

namespace mongo {

/**
 *  ARTree factory method
 */
ARTree* ARTree::create(ARTreeKVEngine* engine, uint64_t cappedMaxSize, uint64_t cappedMaxDocs) {
    uint64_t max = cappedMaxSize;
    bool isCapped = false;

    // round lowest offset in arena zero up to a multiple of 8
    uint32_t offset = (sizeof(ARTArena) + 7) & -8U;

    if (!max)
        max = 1ULL * 1024 * 1024;  // initial allocation request
    else {
        isCapped = true;
        if (max < 4096) {
            cappedMaxSize = 4096;
            max = 4096;
        }
        if (!cappedMaxDocs)
            cappedMaxDocs = cappedMaxSize / 5;  // min BSON object size is 5

        //  calculate capped size of initial artree and arena zero

        max = sizeof(ARTree)                     // artree w/1K arena pointers
            + cappedMaxDocs * sizeof(ARTRecord)  // record array in arena zero
            + offset;                            // arena zero overhead

        // round initial request up to a multiple of 4096
        max += 4095;
        max &= -4096ULL;
    }

    ARTree* art = static_cast<ARTree*>(ARTreeAlloc(max));
    art->_arena[0] = reinterpret_cast<ARTArena*>(art + 1);
    art->_initialSize = max;
    art->_engine = engine;

    max -= sizeof(ARTree);  // calc size of arena zero

    art->_arena[0]->_records =
        reinterpret_cast<ARTRecord*>(reinterpret_cast<uint8_t*>(art->_arena[0]) + max);

    art->_arena[0]->_arenaMax = max;

    art->_docSize = 0;
    art->_docCount = 0;
    art->_nClients = 0;
    art->_isClosed = false;
    art->_catalogCollection = false;

    art->_arenaNext.off = 0;  // sets _segment to 0
    art->_arenaNext._offset = offset >> 3;
    memset(art->_cursors, 0, sizeof(ARTreeOrderedList));

    if (isCapped) {
        art->_cappedCollection = true;
        art->_cappedMaxSize = cappedMaxSize;
        art->_cappedMaxDocs = cappedMaxDocs;
        art->arenaExtend(0);  // create one new arena for docs and indexes
                              // arena 0 contains record array only
    }

    return art;
}

//
//  close all arenas/deallocate ARTree
//
void ARTree::close() {
    if (!_isClosed)
        return;

    while (_currentArena) {
        ARTreeFree(_arena[_currentArena], static_cast<size_t>(_arena[_currentArena]->_arenaMax));
        _currentArena--;
    }

    ARTreeFree((uint8_t*)&_arena[0], static_cast<size_t>(_initialSize));
}

//
//  reinitialize, but do not deallocate
//
void ARTree::truncate() {
    while (_currentArena) {
        ARTreeFree(_arena[_currentArena], static_cast<size_t>(_arena[_currentArena]->_arenaMax));
        _currentArena--;
    }

    // round up to a multiple of 8

    uint32_t offset = sizeof(ARTArena);

    if (uint32_t amt = offset & 0x7)
        offset += 8 - amt;

    _arenaNext.off = 0;  // sets _segment to 0
    _arenaNext._offset = offset >> 3;

    _arena[0]->_arenaRec.off = 0;  // reset next recordId

    _maxArenaDouble = 0;
    _cappedBase = 0;
    _cappedRec = 0;
    _docCount = 0;
    _docSize = 0;

    memset(_uncommittedRecs, 0, sizeof(_uncommittedRecs));  // uncommitted queue
    memset(_cursors, 0, sizeof(_cursors));                  // queue of outstanding cursors
    memset(_freeFrame, 0, sizeof(_freeFrame));              // free chain of ARTFrames
    memset(_tailNode, 0, sizeof(_tailNode));                // tail of wait frames of nodes
    memset(_headNode, 0, sizeof(_headNode));                // head of wait frames of nodes
    memset(_freeNode, 0, sizeof(_freeNode));                // free frames of artree nodes
    memset(_tailFreeStorage, 0, sizeof(_tailFreeStorage));  // tail of wait frame storage
    memset(_headFreeStorage, 0, sizeof(_headFreeStorage));  // head of wait frame storage
    memset(_freeStorage, 0, sizeof(_freeStorage));          // free frames of storage by power of 2
    memset(_tailRecId, 0, sizeof(_tailRecId));              // tail of wait frame record ids
    memset(_headRecId, 0, sizeof(_headRecId));              // head of wait frame record ids
    memset(_freeRecId, 0, sizeof(_freeRecId));              // free frames of record ids

    if (_cappedCollection)
        arenaExtend(0);  // create one new arena for docs and indexes
}

//
//  extend another arena
//  doubling the current arena size
//
void ARTree::arenaExtend(uint32_t size) {
    uint64_t max = _initialSize << _maxArenaDouble;
    uint32_t cnt = 0;

    //  double the current size up to 64GB
    //  making sure that size bytes are available

    do
        max <<= 1, cnt++;
    while (max < size + sizeof(ARTArena));

    if (max < 1ULL * 1024 * 1024)
        max = 1ULL * 1024 * 1024;

    if (max > 64ULL * 1024 * 1024 * 1024)
        max = 64ULL * 1024 * 1024 * 1024;
    else
        _maxArenaDouble += cnt;

    void* artData = ARTreeAlloc(max);
    _arena[_currentArena + 1] = static_cast<ARTArena*>(artData);

    _arena[_currentArena + 1]->_records =
        reinterpret_cast<ARTRecord*>(reinterpret_cast<uint8_t*>(artData) + max);

    _arena[_currentArena + 1]->_arenaRec.off = 0;
    _arena[_currentArena + 1]->_arenaRec._segment = _currentArena + 1;
    _arena[_currentArena + 1]->_arenaMax = max;

    // round up to a multiple of 8
    uint32_t offset = sizeof(ARTArena);

    if (uint32_t amt = offset & 0x7)
        offset += 8 - amt;

    _arenaNext._offset = offset >> 3;
    _arenaNext._segment++;
    _currentArena++;
}

//
//  return node address from offset
//
void* ARTree::arenaSlotAddr(ARTAddr addr) const {
    return reinterpret_cast<uint8_t*>(_arena[addr._segment]) + addr._offset * 8ULL;
}

//
//  return node address from slot entry
//
void* ARTree::arenaSlotAddr(ARTSlot* slot) const {
    return arenaSlotAddr(slot->addr);
}

//
//  allocate space in current arena heap
//
ARTAddr ARTree::arenaAlloc(uint32_t size) {
    MutexSpinLock::lock(_arenaMutex);  // TODO: see if this can be replaced with raii pattern
    ARTAddr addr;

    if (uint32_t amt = size & 0x7)
        size += 8 - amt;

    uint64_t max = _arena[_currentArena]->_arenaMax -
        _arena[_currentArena]->_arenaRec._offset * sizeof(ARTRecord);

    // see if we are colliding in the middle

    if (_arenaNext._offset * 8ULL + size > max)
        arenaExtend(size);

    addr = _arenaNext;
    _arenaNext._offset += size >> 3;
    MutexSpinLock::unlock(_arenaMutex);
    return addr;
}

//
//  return node set to use
//
uint32_t ARTree::arenaNodeSet() {
    return _arenaSet++ % artree::nBuckets;
}

//
//  initialize new frame contents
//
void ARTree::initFrame(ARTSlot* queue, uint32_t size, uint32_t dup) {
    ARTAddr addr = arenaAlloc(size * dup);  // allocate FrameSlots nodes
    ARTFrame* frame = arenaFrameAddr(queue);
    queue->nslot = 0;

    while (dup--) {
        frame->_slots[queue->nslot].addr.off = 0;
        frame->_slots[queue->nslot].addr._offset = (addr._offset * 8uLL + size * dup) >> 3;
        frame->_slots[queue->nslot++].addr._segment = addr._segment;
    }
}

//
//  allocate a new node in the thread's set of the arena
//
ARTAddr ARTree::arenaAllocNode(uint32_t set, uint8_t type) {
    uint32_t size;
    uint32_t dup = FrameSlots;

    // may optimize this: as typeSize[ type ] with static initializer
    switch (type) {
        case SpanNode:
            size = sizeof(ARTspan);
            break;
        case Array4:
            size = sizeof(ARTnode4);
            break;
        case Array14:
            size = sizeof(ARTnode14);
            break;
        case Array64:
            size = sizeof(ARTnode64);
            break;
        case Array256:
            size = sizeof(ARTnode256);
            break;
        case Optime:
            size = sizeof(ARTOplog);
            break;
        default:
            MONGO_UNREACHABLE;
    }

    if (uint32_t xtra = size & 0x7)
        size += (8 - xtra);

    ARTSlot* queue = &_freeNode[set][type];
    MutexSpinLock::lock(queue);  // note: we unlock this elsewhere
    ARTSlot slot[1];

    //  allocate initially empty frame

    if (!queue->off)
        queue->addr = arenaAlloc(sizeof(ARTFrame));

    //  see if there is a free node in the Frame

    while (!getSlotFromFrame(queue, slot)) {
        if (!getFrameFromNodeWait(queue, &_tailNode[set][type]))
            initFrame(queue, size, dup);
    }

    MutexSpinLock::unlock(queue);
    memset(arenaSlotAddr(slot), 0, size);
    return slot->addr;
}

//
//  allocate storage space
//
ARTAddr ARTree::arenaAllocStorage(uint32_t set, uint8_t bits) {
    //  process wait queue
    while (processStorageQueue(set, &_headFreeStorage[set], &_tailFreeStorage[set]))
        ;

    uint32_t size = 1U << bits;
    uint32_t dup = FrameSlots;

    if (bits > 16) {
        set = 0;
        dup = 1;
    }

    ARTSlot* queue = &_freeStorage[set][bits];
    MutexSpinLock::lock(queue);  // TODO: see if this can be replaced with raii pattern
    ARTSlot slot[1];

    if (!queue->off)
        queue->addr = arenaAlloc(sizeof(ARTFrame));

    while (!getSlotFromFrame(queue, slot))
        initFrame(queue, size, dup);

    MutexSpinLock::unlock(queue);
    return slot->addr;
}

Status ARTree::getMetadata(OperationContext* optCtx, BSONObjBuilder* builder) {
    builder->append("size", static_cast<long long>(getSize()));
    builder->append("count", static_cast<long long>(getCount()));
    return Status::OK();
}

//
// debugging helpers
//

std::string indent(uint32_t depth) {
    std::ostringstream oss;
    oss << '\n';
    for (uint32_t i = 0; i < depth; i++)
        oss << "  ";
    oss << "[" << depth << "]";
    return oss.str();
}

std::string ARTree::toHex(uint64_t n) {
    std::ostringstream oss;
    if (static_cast<int64_t>(n) < 0) {
        oss << static_cast<int64_t>(n);
    } else {
        oss << "0x" << std::hex << n;
    }
    return oss.str();
}

std::string ARTree::keyStr(const uint8_t* key, uint32_t keylen) {
    std::ostringstream oss;
    oss << "(";
    for (uint32_t i = 0; i < keylen; i++) {
        if (i > 0)
            oss << ',';
        oss << ARTree::toHex(static_cast<uint64_t>(key[i]) & 0xff);
    }
    oss << ")";
    return oss.str();
}

std::string ARTree::keyStr(const char* key, uint32_t keylen) {
    std::ostringstream oss;
    oss << "(";
    for (uint32_t i = 0; i < keylen; i++) {
        if (i > 0)
            oss << ',';
        oss << ARTree::toHex(static_cast<uint64_t>(key[i]) & 0xff);
    }
    oss << ")";
    return oss.str();
}

std::string ARTree::toHexByte(uint8_t n) {
    std::ostringstream oss;
    oss << "0x" << std::hex << n;
    return oss.str();
}

std::string ARTree::byteStr(uint8_t* bytes) const {
    std::ostringstream oss;
    for (uint32_t i = 0; i < 8; i++) {
        if (i > 0)
            oss << ',';
        oss << toHex(static_cast<uint64_t>(bytes[i]));
    }
    return oss.str();
}

std::string ARTree::dataStr(const char* bytes, uint32_t len) {
    std::ostringstream oss;
    for (uint32_t i = 0; i < len; i++) {
        if (i > 0)
            oss << ',';
        oss << toHex(static_cast<uint64_t>(bytes[i]));
    }
    return oss.str();
}

std::string ARTree::bsonStr(const char* bytes) {
    return BSONObj(bytes).toString(0, 0);
}

std::string ARTree::mutexStr(uint8_t mutex) const {
    std::ostringstream oss;
    oss << (mutex ? "LOCKED" : "UNLOCKED");
    return oss.str();
}

std::string ARTree::slotTypeStr(enum NodeType type) const {
    std::ostringstream oss;
    switch (type) {
        case UnusedSlot:
            oss << "UnusedSlot";
            break;
        case SpanNode:
            oss << "SpanNode";
            break;
        case Array4:
            oss << "Array4";
            break;
        case Array14:
            oss << "Array14";
            break;
        case Array64:
            oss << "Array64";
            break;
        case Array256:
            oss << "Array256";
            break;
        case EndKey:
            oss << "EndKey";
            break;
        case MaxNode:
            oss << "MaxNode";
            break;
        default:
            oss << "UnrecognizedNodeType";
            break;
            ;
    }
    return oss.str();
}

std::string ARTree::valueStr(ARTSlot* slot, uint64_t len) const {
    std::ostringstream oss;
    uint8_t* b = static_cast<uint8_t*>(arenaSlotAddr(slot));
    for (uint32_t i = 0; i < len; i++) {
        if (i > 0)
            oss << ',';
        oss << toHex(static_cast<uint64_t>(b[i]));
    }
    return oss.str();
}

std::string ARTree::slotStr(ARTSlot* slot, bool valueBit) const {
    std::ostringstream oss;
    oss << "[offset = " << toHex(static_cast<uint64_t>(slot->off));
    if (valueBit)
        oss << ", value = (" << valueStr(slot, 1 << slot->nbits) << ")"
            << ", type = " << slotTypeStr(static_cast<enum NodeType>(slot->type))
            << ", mutex = " << mutexStr(slot->mutex)
            << ", nslot = " << static_cast<uint64_t>(slot->nslot) << ", dead = " << slot->dead
            << "]";
    return oss.str();
}

void ARTree::printCursor(ARTreeCursor* cursor) const {
    std::ostringstream oss;
    oss << "\n\nARTreeCursor["
        << "\n  current depth of cursor = " << cursor->_depth
        << "\n  current size of key = " << cursor->_keySize
        << "\n  maximum depth of cursor = " << cursor->_maxDepth
        << "\n  leftEOF = " << cursor->_atLeftEOF << "\n  rightEOF = " << cursor->_atRightEOF
        << "\n  EOF = " << cursor->_atEOF << "\n  recordId = " << cursor->_recordId.off
        << "\n  key = ";
    for (uint32_t i = 0; i < cursor->_keySize; i++) {
        if (i > 0)
            oss << ',';
        oss << toHex(static_cast<uint64_t>(cursor->_key[i]));
    }
    oss << "\n  current cursor recordId = " << cursor->_recordId.off;
    log() << oss.str();
    printStack(cursor->_stack, cursor->_depth);
}

void ARTree::printStack(ARTreeStack* stack, uint64_t depth) const {
    std::ostringstream oss;
    oss << "\n\nARTStack[";
    for (uint32_t i = 0; i < depth; i++) {
        oss << "\n  key = " << toHex(static_cast<uint64_t>(stack[i].ch)) << ", "
            << slotStr(stack[i].slot);
    }
    oss << "\n]" << std::endl;
    log() << oss.str();
}

void ARTree::printTrie(ARTSlot* slot, uint32_t depth) const {
    log() << "\nARTree index" << trieStr(slot, depth);
}

std::string ARTree::trieStr(ARTSlot* slot, uint32_t depth) const {
    std::ostringstream oss;

    ARTSlot* p = slot;

    oss << indent(depth) << slotStr(p);

    switch (p->type) {
        case EndKey: {
            oss << indent(depth) << "END";
            break;
        }
        case SpanNode: {
            ARTspan* spanNode = static_cast<ARTspan*>(arenaSlotAddr(p));
            oss << indent(depth) << "bytes = " << byteStr(spanNode->bytes);
            if (spanNode->next->type) {
                oss << indent(depth) << " ->" << trieStr(spanNode->next, depth);
            }
            break;
        }
        case Array4: {
            ARTnode4* radix4Node = static_cast<ARTnode4*>(arenaSlotAddr(p));
            for (uint32_t i = 0; i < 4; i++)
                if (radix4Node->alloc & (1 << i)) {
                    oss << indent(depth) << "keys[" << i
                        << "] = " << toHex(static_cast<uint64_t>(radix4Node->keys[i]))
                        << indent(depth) << "child[" << i
                        << "] =" << trieStr(&radix4Node->radix[i], depth + 1);
                }
            break;
        }
        case Array14: {
            ARTnode14* radix14Node = static_cast<ARTnode14*>(arenaSlotAddr(p));
            for (uint32_t i = 0; i < 14; i++)
                if (radix14Node->alloc & (1 << i)) {
                    oss << indent(depth) << "keys[" << i
                        << "] = " << toHex(static_cast<uint64_t>(radix14Node->keys[i]))
                        << indent(depth) << "child[" << i
                        << "] =" << trieStr(&radix14Node->radix[i], depth + 1);
                }
            break;
        }
        case Array64: {
            ARTnode64* radix64Node = static_cast<ARTnode64*>(arenaSlotAddr(p));
            for (uint32_t i = 0; i < 256; i++)
                if (radix64Node->alloc & (1ULL << radix64Node->keys[i])) {
                    oss << indent(depth) << "child[" << toHex(i)
                        << "] =" << trieStr(&radix64Node->radix[radix64Node->keys[i]], depth + 1);
                }
            break;
        }
        case Array256: {
            ARTnode256* radix256Node = static_cast<ARTnode256*>(arenaSlotAddr(p));
            for (uint32_t i = 0; i < 256; i++)
                if (radix256Node->alloc[i / 64] & (1ULL << (i % 64))) {
                    oss << indent(depth) << "child[" << toHex(i)
                        << "] =" << trieStr(&radix256Node->radix[i], depth + 1);
                }
            break;
        }
        case UnusedSlot: {
            break;
        }
        default:
            ;
    }  // end switch

    return oss.str();
}

void ARTree::printIndex(ARTreeIndex* index) const {
    std::ostringstream oss;
    oss << "\n\nARTreeIndex[ (" << reinterpret_cast<uint64_t>(index->_root) << ")\n";
    oss << trieStr(index->_root, 0);
    oss << "]\n";
    log() << oss.str();
}

std::string ARTree::recordStr(ARTRecord* rec) const {
    std::ostringstream oss;
    oss << "\n  ARTRecord["
        << "\n    timestamp = " << rec->_timestamp << "\n    txnTime = " << rec->_deadstamp
        << "\n    used = " << rec->_used << "\n    doc slot = " << slotStr(rec->_doc)
        << "\n    oldDoc slot = " << slotStr(rec->_oldDoc) << ']';
    return oss.str();
}

void ARTree::printRecord(ARTRecord* rec) const {
    log() << recordStr(rec);
}

void ARTree::printRecords(ARTRecord* records, uint64_t maxId) const {
    uint64_t recId = 0;
    while (++recId <= maxId) {
        ARTRecord* rec = static_cast<ARTRecord*>(records - recId);
        printRecord(rec);
    }
}

std::string ARTree::discStr(KeyString::Discriminator disc) {
    std::ostringstream oss;
    switch (disc) {
        case KeyString::kInclusive:
            oss << "Inclusive";
            break;
        case KeyString::kExclusiveBefore:
            oss << "ExclusiveBefore";
            break;
        case KeyString::kExclusiveAfter:
            oss << "ExclusiveAfter";
            break;
        default:
            ;
    }
    oss << std::endl;
    return oss.str();
}

}  // namespace mongo
