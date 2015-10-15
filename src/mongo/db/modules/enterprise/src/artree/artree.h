//@file artree.h
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

#pragma warning(disable : 4200)
#pragma once

#include <cstdint>
#include <atomic>

#include "mongo/bson/ordering.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/capped_callback.h"
#include "mongo/db/storage/key_string.h"

#include "artree_nodes.h"
#include "artree_common.h"
#include "artree_frame.h"
#include "artree_index.h"
#include "artree_kv_engine.h"
#include "artree_ordered_list.h"
#include "artree_records.h"
#include "artree_recovery_unit.h"

namespace mongo {

namespace artree {
static const int maxArenas = 1024;  // maximum number of 32GB Arenas in a collection
static const int nBuckets = 19;     // number of distinct sets - increase for higher core count
                                    // or better: just use 256, and pick a set with getcpu
}

/**
 *  The ART (Adaptive Radix Tree) containing the heap storage management.
 *
 *  Each arena corresponds to one collection.
 *
 *  The arena is allocated at init for each storage engine instance.
 *  There is no realloc of the arenas; when they are full, the engine
 *  may allocate a new arena extension, or return an error.
 *
 *  'arenaNext' is synchonized via arenaMutex.
 *
 *  freeNodes are partitioned into 19 sets, of MaxNode types.
 *  These set are intended to minimize thread cache contention
 *  by avoiding the consecutive allocation of contiguous storage.
 *
 *  freeSpace contains all free space nodes by size: (1<<3) up to (1<<32)
 *
 *  txnId is the global (next) transaction number, allocated to each new
 *  request that is doing a node insert.  Odd txn are writes, even reads
 *
 *  MVCC semantics requires that we retain nodes and docs until all active
 *  transaction ids are larger than the delete object's txn id.
 *  Currently active cursors are linked together in a priorty-ordered
 *  linked list, known internally as an ARTreeOrderedList.
 */

#define STORE_DOC true
#define STORE_TYPEBITS false

class ARTreeStack;

/* Arena:

     low                 (1)               (2)       high addr
    [                    -->               <--           ]
    [ data| d |   d   |d|...               ...|r|r|r|rec ]
    [       ^__________________________________|   ^     ]
                                                   |
                                                recordID

    (1) = _arenaNext, increment offset
    (2) = _arenaRec,  increment offset and subtract from high address

    'data' includes BSON docs and index nodes of all types
*/

struct ARTArena {
    uint64_t _arenaMax;  // arena size
    ARTAddr _arenaRec;   // highest Record ID in issued

    ARTRecord* _records;  // array of fixed-size RecordId metadata
                          // allocated downward from top of the arena
                          // node / document heap is allocated upward
                          // from the bottom of the arena
};

struct ARTree {
    bool _isClosed;                       // 'drop collection' was called
    std::atomic_uint_fast64_t _nClients;  // number of open cursors, rec stores, indexes

    ARTArena* _arena[artree::maxArenas];  // pointers to allocated arena
    ARTAddr _arenaNext;                   // next available arena node offset
    uint64_t _initialSize;                // initial allocation size
    uint32_t _maxArenaDouble;             // maximum number of size doubles
    uint32_t _currentArena;               // current (highest) arena index

    std::atomic_uint_fast64_t _cappedBase;  // lowest recId in the span
    std::atomic_uint_fast64_t _cappedRec;   // highest recId in the span
    std::atomic_uint_fast64_t _docCount;    // number of docs
    std::atomic_uint_fast64_t _docSize;     // sum of doc sizes
    // std::atomic_uint_fast64_t _nClients;  // number of open cursors, rec stores, indexes

    ARTreeKVEngine* _engine;  // the storage engine for the database
    uint64_t _cappedMaxDocs;  // maximum number of docs in capped collection
    uint64_t _cappedMaxSize;  // maximum size of capped collection

    ARTreeOrderedList _uncommittedRecs[1];  // Capped Collection open transactions
    ARTreeOrderedList _cursors[1];          // cursor priority queue
    ARTSlot _freeFrame[1];                  // ARTFrames free list

    ARTSlot _tailNode[artree::nBuckets][MaxNode];  // tail of wait frames of nodes
    ARTSlot _headNode[artree::nBuckets][MaxNode];  // head of wait frames of nodes
    ARTSlot _freeNode[artree::nBuckets][MaxNode];  // free frames of artree nodes

    ARTSlot _tailFreeStorage[artree::nBuckets];  // tail of wait frame storage
    ARTSlot _headFreeStorage[artree::nBuckets];  // head of wait frame storage
    ARTSlot _freeStorage[artree::nBuckets][32];  // free frames of storage by power of 2

    ARTSlot _tailRecId[artree::nBuckets];  // tail of wait frame record ids
    ARTSlot _headRecId[artree::nBuckets];  // head of wait frame record ids
    ARTSlot _freeRecId[artree::nBuckets];  // free frames of record ids

    char _oplogMutex[1];      // oplog trim concurrency
    char _arenaMutex[1];      // arena allocation concurrency
    bool _cappedCollection;   // collection is capped
    bool _catalogCollection;  // collection is a catalog
    uint8_t _arenaSet;        // free node set being used

    /**
     * factory method
     * @param max  - the size (MB) of the first in-memory storage engine arena
     */
    static ARTree* create(ARTreeKVEngine* engine = nullptr,
                          uint64_t cappedMaxSize = 0,
                          uint64_t cappedMaxDocs = 0);

    /**
     * unlock ART resources
     */
    void close();
    bool isClosed() const { return _isClosed; }

    /**
     * even => (uncommitted) reader timestamps
     * odd  =>  committed writer timestamps
     */
    static bool isReader(uint64_t ts) {
        return !(ts & 1);
    }
    static bool isCommitting(uint64_t ts) {
        return ts && !(ts & 1);
    }

    static bool isWriter(uint64_t ts) {
        return (ts & 1);
    }
    static bool isCommitted(uint64_t ts) {
        return (ts & 1);
    }

    /**
     * return frame address from slot
     */
    ARTFrame* arenaFrameAddr(ARTSlot* slot) const;

    /**
     * return node address from address
     */
    void* arenaSlotAddr(ARTAddr addr) const;

    /**
     * return node address from slot
     */
    void* arenaSlotAddr(ARTSlot* slot) const;

    /**
     * alocate node space in the Arena heap
     */
    ARTAddr arenaAlloc(uint32_t size);

    //
    //  extend another arena
    //
    void arenaExtend(uint32_t size);

    /**
     * allocate storage space in the Arena heap
     */
    ARTAddr arenaAllocStorage(uint32_t set, uint8_t bits);

    /**
     * return record space in the Arena heap
     */
    void arenaFreeStorage(uint32_t set, ARTSlot* node);

    /**
     * allocate a new ART node in the Arena heap
     */
    ARTAddr arenaAllocNode(uint32_t set, uint8_t type);

    /**
     * initialize a new ART frame with slots from the heap
     */
    void initFrame(ARTSlot* queue, uint32_t size, uint32_t dup);

    /**
     * get a free ARTFrame node from the Frame heap
     */
    ARTAddr arenaAllocFrame();

    /**
     * put empty frame on free list
     */
    void putFrameOnFreeList(ARTFrame* frame, ARTAddr addr);

    /**
     * return the current arena node set
     */
    uint32_t arenaNodeSet();

    /**
     * add slot to free/wait frame
     */
    void addSlotToFrame(ARTSlot* head, ARTSlot* tail, ARTSlot* slot);

    /**
     * add slot to free/wait frame
     */
    void addSlotToFrame(ARTSlot*, ARTSlot*);

    /**
     * get slot from free/wait frame
     */
    bool getSlotFromFrame(ARTSlot* wait, ARTSlot* slot);

    /**
     * advance wait frame to free list
     */
    bool getFrameFromNodeWait(ARTSlot* queue, ARTSlot* tail);

    static ARTreeRecoveryUnit* recUnit(OperationContext* opCtx) {
        return dynamic_cast<ARTreeRecoveryUnit*>(opCtx->recoveryUnit());
    }
    /**
     * process storage nodes to from wait queue to free list
     */
    bool processStorageQueue(uint32_t set, ARTSlot* queue, ARTSlot* tail);

    ARTAddr allocRecId(uint32_t set);

    /**
     * validate a RecordId
     */
    bool isValidRecId(uint64_t loc) const;

    /**
     * store a document into the arena heap
     */
    ARTDocument* storeDocument(
        ARTSlot* slot, uint32_t set, const uint8_t* data, uint32_t len, bool isDoc);

    /**
     * update a document in the arena heap
     */
    bool updateRec(OperationContext* opCtx,
                   uint64_t ts,
                   uint32_t set,
                   bool isLocked,
                   const uint8_t* data,
                   uint32_t len,
                   uint64_t prevId);

    /**
     * commit an insert of a document
     */
    void commitInsertRec(uint64_t loc);

    /**
     * commit an update to a document
     */
    void commitUpdateRec(uint32_t set, uint64_t loc, uint64_t old);

    /**
     * roll back an update to an arena document
     */
    void unUpdateRec(uint32_t set, uint64_t loc);

    /**
     * store arbitrary data into the arena
     */
    uint64_t storeRec(uint32_t set, const uint8_t* data, uint32_t len, OperationContext* opCtx);

    /**
     * delete data from the arena
     */
    void deleteRec(uint32_t set, uint64_t loc);

    /**
     * record cleanup handlers
     */
    bool processRecIdQueue(uint32_t set, ARTSlot* queue, ARTSlot* tail);
    void processRecordEntry(ARTAddr recId, uint32_t set);

    /**
     * transaction timestamp allocators
     */
    void assignLatestTimestamp(uint64_t* timestamp);
    uint64_t assignTxnId(ARTRecord* rec);
    void assignDeadstampId(uint64_t loc);

    /**
    * increment or decrement a recordId
    * return false at either end
    */
    bool incr(ARTAddr* recId);
    bool decr(ARTAddr* recId);

    /**
     * return the record corresponding to a given record id / loc
     */
    ARTRecord* fetchRec(uint64_t loc) const;
    ARTRecord* fetchRec(ARTAddr recId) const;
    ARTAddr followVersionChain(void* recUnit, uint64_t txnId, ARTAddr recId);
    bool isVisibleVersion(void* recUnit, uint64_t txnId, ARTAddr recId);

    /**
     * restore capped collection size invariants
     */
    void cappedTrim(OperationContext* opCtx, uint32_t set, CappedCallback* callback);

    /**
     * fetch the document corresponding to a given reference object
     */
    ARTDocument* fetchDoc(uint64_t loc) const;
    ARTDocument* fetchDoc(ARTSlot* slot) const;
    ARTDocument* fetchDoc(ARTAddr recId) const;
    ARTDocument* fetchDoc(ARTRecord* rec) const;

    /**
     * queue handing capped collection uncommitted record ids
     */
    void enqueueUncommittedRecord(uint64_t loc);
    void dequeueUncommittedRecord(uint64_t loc);

    /**
     * queue handling for cursors (and iterators)
     */
    void enqueueCursor(ARTreeOrderedList::Entry* queue);
    void dequeueCursor(ARTreeOrderedList::Entry* queue);

    /**
     * return the exact size in bytes of all documents in the arena
     */
    uint64_t getSize() const {
        return _docSize;
    }

    /**
     * return the exact number of all documents in the arena
     */
    uint64_t getCount() const {
        return _docCount;
    }

    /**
     * set the size in bytes of all documents in the arena
     */
    void setSize(uint64_t size) {
        _docSize = size;
    }

    /**
     * set the count of all documents in the arena
     */
    void setCount(uint64_t count) {
        _docCount = count;
    }

    /**
     * return true if the arena represents a capped collection
     */
    bool isCapped() const {
        return _cappedCollection;
    }

    /**
     * handle collection truncation
     */
    void truncate();

    /**
     * return collection stats
     */
    Status getMetadata(OperationContext*, BSONObjBuilder*);

    // debugging helpers
    static std::string toHex(uint64_t n);
    static std::string toHexByte(uint8_t n);
    static std::string keyStr(const uint8_t* key, uint32_t keylen);
    static std::string keyStr(const char* key, uint32_t keylen);
    static std::string discStr(KeyString::Discriminator disc);
    static std::string dataStr(const char* bytes, uint32_t len);
    static std::string bsonStr(const char* bytes);

    std::string byteStr(uint8_t* bytes) const;
    std::string mutexStr(uint8_t mutex) const;
    std::string slotStr(ARTSlot* slot, bool valueBit = false) const;
    std::string slotTypeStr(enum NodeType type) const;
    std::string trieStr(ARTSlot* slot, uint32_t depth) const;
    std::string valueStr(ARTSlot* slot, uint64_t len) const;
    std::string recordStr(ARTRecord* rec) const;

    void printCursor(ARTreeCursor* cursor) const;
    void printIndex(ARTreeIndex* index) const;
    void printRecord(ARTRecord* rec) const;
    void printRecords(ARTRecord* records, uint64_t maxId) const;
    void printStack(ARTreeStack* stack, uint64_t depth) const;
    void printTrie(ARTSlot* slot, uint32_t depth) const;
};

}  // namespace mongo
