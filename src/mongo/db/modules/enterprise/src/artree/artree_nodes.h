//@file artree_nodes.h
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

#include <string>
#include <cstdint>

#include "artree_common.h"

namespace mongo {

enum NodeType {
    // Artree nodes interior to tree (3 bits max)
    UnusedSlot = 0,  // slot is not yet in use
    SpanNode,        // node contains up to 8 key bytes
    Array4,          // node contains 4 radix slots
    Array14,         // node contains 14 radix slots
    Array64,         // node contains 64 radix slots
    Array256,        // node contains 256 radix slots
    EndKey,          // slot marks the end of a key
    Optime,          // slot marks the end of an optime
    MaxNode          // maximum node type
};

static const uint8_t MUTEX_BIT = 0x1;
static const uint8_t DEAD_BIT = 0x2;

typedef union {
    struct {
        uint64_t off : 48;   // offset to node sub-contents
        uint64_t mutex : 1;  // update/write synchronization
        uint64_t dead : 1;   // node is no longer in the tree
        uint64_t type : 6;   // type of radix node
        uint64_t fill : 8;   // filler for union below
    };
    volatile uint8_t bytes[8];
    volatile uint64_t bits;
    ARTAddr addr;
    struct {
        uint8_t filler[6];
        volatile char latch[1];
        union {
            uint8_t nbits;  // number of bits log_2(document length)
            uint8_t nbyte;  // number of bytes in a SpanNode
            uint8_t nslot;  // number of slots of frame in use
            uint8_t state;  // state of an oplog node
        };
    };
} ARTSlot;

class SlotUtilities {
public:
    static std::string slot2String(ARTSlot* slot);
};

struct ARTree;
class ARTreeCursor;

/**
 *  Used by thread to pass state into node methods
 */
struct ParamStruct {
    void fillKey();

    ARTSlot* slot;
    ARTSlot* prev;
    ARTSlot newSlot[1];

    ARTreeCursor* cursor;
    const uint8_t* key;
    ARTree* art;
    uint32_t keylen;  // length of the key
    uint32_t depth;   // current tree depth
    uint32_t off;     // progress down the key bytes
    uint32_t set;     // allocaton set for inserts
    uint8_t ch;       // current node character

    friend std::ostream& operator<<(std::ostream& os, const ParamStruct& s);
};

/*
 * radix tree traversal states
 */
typedef enum { ContinueSearch, EndSearch, RetrySearch, RestartSearch } ReturnState;

/**
 * generic trie node
 */
struct ARTnode {
    ARTSlot free[1];
};

/**
 * radix node with four slots and their key bytes
 */
struct ARTnode4 {
    volatile uint8_t alloc;
    volatile uint8_t keys[4];
    uint8_t filler[3];
    ARTSlot radix[4];

    ReturnState findKey(ParamStruct* p);
    ReturnState insertKey(ParamStruct* p);
    ReturnState deleteKey(ParamStruct* p);
};

/**
 * radix node with fourteen slots and their key bytes
 */
struct ARTnode14 {
    volatile uint16_t alloc;
    volatile uint8_t keys[14];
    ARTSlot radix[14];

    ReturnState findKey(ParamStruct* p);
    ReturnState insertKey(ParamStruct* p);
    ReturnState deleteKey(ParamStruct* p);
};

/**
 * radix node with sixty-four slots and a 256 key byte array
 */
struct ARTnode64 {
    volatile uint64_t alloc;
    volatile uint8_t keys[256];
    ARTSlot radix[64];

    ReturnState findKey(ParamStruct* p);
    ReturnState insertKey(ParamStruct* p);
    ReturnState deleteKey(ParamStruct* p);
};

/**
 * radix node all two hundred fifty six slots
 */
struct ARTnode256 {
    volatile uint64_t alloc[4];
    ARTSlot radix[256];

    ReturnState findKey(ParamStruct* p);
    ReturnState insertKey(ParamStruct* p);
    ReturnState deleteKey(ParamStruct* p);
};

/**
 * span node containing up to 8 consecutive key bytes
 * span nodes are used to compress linear chains of key bytes
 */
struct ARTspan {
    uint8_t bytes[8];
    ARTSlot next[1];  // next node under span

    ReturnState findKey(ParamStruct* p);
    ReturnState insertKey(ParamStruct* p);
    ReturnState deleteKey(ParamStruct* p);
};

}  // namespace mongo
