//@file artree_nodes.cpp
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

#include <cstdlib>
#include <cstring>
#include <sstream>

#include "artree_cursor.h"
#include "artree_nodes.h"
#include "artree.h"
#include "artree_util.h"

#ifdef WIN32
#include "mongo/platform/bits.h"
#endif

namespace mongo {

std::string SlotUtilities::slot2String(ARTSlot* s) {
    std::ostringstream oss;
    oss << "ARTSlot["
        << " off=" << s->off << ",type=" << s->type << ",nbyte=" << s->nbyte << ",dead=" << s->dead
        << " ]";
    return oss.str();
}

//
// remove span next node from trie
//
ReturnState ARTspan::deleteKey(ParamStruct* p) {
    MutexSpinLock::kill(p->slot);
    p->art->addSlotToFrame(
        &p->art->_headNode[p->set][SpanNode], &p->art->_tailNode[p->set][SpanNode], p->newSlot);
    return ContinueSearch;
}

//
// remove key from radix4 node
//
ReturnState ARTnode4::deleteKey(ParamStruct* p) {
    uint32_t bit;
    for (bit = 0; bit < 4; bit++) {
        if (alloc & (1 << bit))
            if (p->ch == keys[bit])
                break;
    }

    if (bit == 4)
        return EndSearch;  // key byte not found

    // we are not the last entry in the node?
    alloc &= ~(1 << bit);

    if (alloc)
        return EndSearch;

    MutexSpinLock::kill(p->slot);
    p->art->addSlotToFrame(
        &p->art->_headNode[p->set][Array4], &p->art->_tailNode[p->set][Array4], p->newSlot);
    return ContinueSearch;
}

//
// remove key from radix14 node
//
ReturnState ARTnode14::deleteKey(ParamStruct* p) {
    uint32_t bit;
    for (bit = 0; bit < 14; bit++) {
        if (alloc & (1 << bit))
            if (p->ch == keys[bit])
                break;
    }

    if (bit == 14)
        return EndSearch;  // key byte not found

    alloc &= ~(1 << bit);

    if (alloc)
        return EndSearch;

    MutexSpinLock::kill(p->slot);
    p->art->addSlotToFrame(
        &p->art->_headNode[p->set][Array14], &p->art->_tailNode[p->set][Array14], p->newSlot);
    return ContinueSearch;
}

//
// remove key from radix64 node
//
ReturnState ARTnode64::deleteKey(ParamStruct* p) {
    uint32_t bit = keys[p->ch];

    if (bit == 0xff)
        return EndSearch;

    keys[p->ch] = 0xff;
    alloc &= ~(1ULL << bit);

    if (alloc)
        return EndSearch;

    MutexSpinLock::kill(p->slot);
    p->art->addSlotToFrame(
        &p->art->_headNode[p->set][Array64], &p->art->_tailNode[p->set][Array64], p->newSlot);
    return ContinueSearch;
}

//
// remove key from radix256 node
//
ReturnState ARTnode256::deleteKey(ParamStruct* p) {
    uint32_t bit = p->ch;
    if (~alloc[bit / 64] & (1ULL << (bit % 64)))
        return EndSearch;

    alloc[bit / 64] &= ~(1ULL << (bit % 64));

    if (alloc[0] | alloc[1] | alloc[2] | alloc[3])
        return EndSearch;

    MutexSpinLock::kill(p->slot);
    p->art->addSlotToFrame(
        &p->art->_headNode[p->set][Array256], &p->art->_tailNode[p->set][Array256], p->newSlot);
    return ContinueSearch;
}

ReturnState ARTnode4::findKey(ParamStruct* p) {
    // simple loop comparing bytes
    uint32_t idx;
    for (idx = 0; idx < 4; idx++) {
        if (alloc & (1 << idx))
            if (p->key[p->off] == keys[idx])
                break;
    }

    if (idx == 4)
        return EndSearch;  // key byte not found

    // byte was found
    p->slot = radix + idx;  // slot points to child node
    p->off++;               // update key byte offset
    if (p->cursor)          // update cursor key
        p->cursor->_key[p->cursor->_keySize++] = keys[idx];

    return ContinueSearch;  // byte found, follow slot to next node
}

ReturnState ARTnode14::findKey(ParamStruct* p) {
    // is key byte in radix node?
    // simple loop comparing bytes
    uint32_t idx;
    for (idx = 0; idx < 14; idx++) {
        if (alloc & (1 << idx))
            if (p->key[p->off] == keys[idx])
                break;
    }

    if (idx == 14)
        return EndSearch;  // key byte not found

    p->slot = radix + idx;  // slot points to child node

    if (p->cursor)  // update cursor key
        p->cursor->_key[p->cursor->_keySize++] = keys[idx];

    p->off++;               // update key byte offset
    return ContinueSearch;  // byte found, follow slot to next node
}

ReturnState ARTnode64::findKey(ParamStruct* p) {
    // is the key byte in this radix node?
    uint32_t idx = keys[p->key[p->off]];  // 256 array of slot idx

    if (idx == 0xff || (~alloc & (1ULL << idx)))
        return EndSearch;  // key byte not found

    p->slot = radix + idx;  // slot points to child node
    if (p->cursor)          // update cursor key
        p->cursor->_key[p->cursor->_keySize++] = p->key[p->off];
    p->off++;  // update key offset

    return ContinueSearch;  // byte found, follow slot to next node
}

ReturnState ARTnode256::findKey(ParamStruct* p) {
    uint32_t idx = p->key[p->off];

    if (~alloc[idx / 64] & (1ULL << (idx % 64)))
        return EndSearch;

    if (p->cursor)  // update cursor key
        p->cursor->_key[p->cursor->_keySize++] = idx;
    p->slot = radix + idx;  // slot points to child node
    p->off++;               // update key byte offset
    return ContinueSearch;
}

ReturnState ARTspan::findKey(ParamStruct* p) {
    uint32_t len = p->newSlot->nbyte;   // span node byte count
    uint32_t amt = p->keylen - p->off;  // remaining key bytes

    if (amt > len)  // use at most key byte count
        amt = len;

    // compare remaining key to span node bytes
    int diff = memcmp(p->key + p->off, bytes, amt);

    if (len > amt) {      // span node covers remaining key bytes
        if (diff <= 0) {  // key <= span bytes
            if (p->cursor)
                p->cursor->_stack[p->cursor->_depth - 1].ch = -1;
        } else {  // diff > 0, key > span bytes
            if (p->cursor)
                p->cursor->_stack[p->cursor->_depth - 1].ch = 256;
        }
        return EndSearch;
    }

    // otherwise (len <= amt), span node does not cover remaining key bytes

    if (diff < 0) {  // key is < span bytes
        if (p->cursor)
            p->cursor->_stack[p->cursor->_depth - 1].ch = -1;
        return EndSearch;
    }

    if (diff > 0) {  // key is > span bytes
        if (p->cursor)
            p->cursor->_stack[p->cursor->_depth - 1].ch = 256;
        return EndSearch;
    }

    // key == span bytes
    if (p->cursor) {
        p->cursor->_stack[p->cursor->_depth - 1].ch = 0;
        memcpy(p->cursor->_key + p->cursor->_keySize, bytes, len);
        p->cursor->_keySize += len;
    }

    p->off += len;
    p->slot = next;
    return ContinueSearch;
}

ReturnState ARTnode4::insertKey(ParamStruct* p) {
    uint32_t idx;
    for (idx = 0; idx < 4; idx++) {
        if (alloc & (1 << idx))
            if (p->key[p->off] == keys[idx]) {
                p->slot = radix + idx;
                p->off++;
                return ContinueSearch;
            }
    }

    // obtain write lock on the node
    MutexSpinLock::lock(p->slot);

    // restart if slot has been killed
    // or node has changed.
    if (p->slot->dead) {
        MutexSpinLock::unlock(p->slot);
        return RestartSearch;
    }

    if (p->slot->off != p->newSlot->off) {
        MutexSpinLock::unlock(p->slot);
        return RetrySearch;
    }

    // retry search under lock
    for (idx = 0; idx < 4; idx++) {
        if (alloc & (1 << idx))
            if (p->key[p->off] == keys[idx]) {
                MutexSpinLock::unlock(p->slot);
                p->slot = radix + idx;
                p->off++;
                return ContinueSearch;
            }
    }

    // add to radix4 node if room
    if (alloc < 0xF) {
// TODO: move this to artree_util, use platform/bits.h abstraction
#ifdef WIN32
        _BitScanForward((DWORD*)&idx, ~alloc);
#else
        idx = __builtin_ctz(~alloc);
#endif

        keys[idx] = p->key[p->off++];
        p->slot = radix + idx;
        p->fillKey();
        alloc |= 1 << idx;
        return EndSearch;
    }

    // the radix node is full, promote to the next larger size.

    ARTAddr addr = p->art->arenaAllocNode(p->set, Array14);
    ARTnode14* radix14Node = static_cast<ARTnode14*>(p->art->arenaSlotAddr(addr));

    p->newSlot->bits = 0;
    p->newSlot->type = Array14;
    p->newSlot->off = addr.off;

    for (idx = 0; idx < 4; idx++) {
        ARTSlot* slot = radix + idx;
        MutexSpinLock::lock(slot);
        if (!slot->dead) {
            uint32_t out;
#ifdef WIN32
            _BitScanForward((DWORD*)&out, ~radix14Node->alloc);
#else
            out = __builtin_ctz(~radix14Node->alloc);
#endif
            radix14Node->alloc |= 1 << out;
            radix14Node->radix[out].bits = slot->bits;  // copies mutex also
            radix14Node->keys[out] = keys[idx];
            radix14Node->radix[out].mutex = 0;
            MutexSpinLock::kill(slot);
        }
        MutexSpinLock::unlock(slot);
    }

    uint32_t out;

// TODO: move this to artree_util, use platform/bits.h abstraction
#ifdef WIN32
    _BitScanForward((DWORD*)&out, ~radix14Node->alloc);
#else
    out = __builtin_ctz(~radix14Node->alloc);
#endif

    radix14Node->keys[out] = p->key[p->off++];

    // fill in rest of the key in span nodes
    p->slot = radix14Node->radix + out;
    p->fillKey();
    radix14Node->alloc |= 1 << out;
    return EndSearch;
}

ReturnState ARTnode14::insertKey(ParamStruct* p) {
    uint32_t idx;

    for (idx = 0; idx < 14; idx++) {
        if (alloc & (1 << idx))
            if (p->key[p->off] == keys[idx]) {
                p->slot = radix + idx;
                p->off++;
                return ContinueSearch;
            }
    }

    // obtain write lock on the node
    MutexSpinLock::lock(p->slot);

    // restart if slot has been killed
    // or node has changed.
    if (p->slot->dead) {
        MutexSpinLock::unlock(p->slot);
        return RestartSearch;
    }

    if (p->slot->off != p->newSlot->off) {
        MutexSpinLock::unlock(p->slot);
        return RetrySearch;
    }

    //  retry search under lock
    for (idx = 0; idx < 14; idx++) {
        if (alloc & (1 << idx))
            if (p->key[p->off] == keys[idx]) {
                MutexSpinLock::unlock(p->slot);
                p->slot = radix + idx;
                p->off++;
                return ContinueSearch;
            }
    }

    // add to radix node if room
    if (alloc < 0x3fff) {
// TODO: move this to artree_util, use platform/bits.h abstraction
#ifdef WIN32
        _BitScanForward((DWORD*)&idx, ~alloc);
#else
        idx = __builtin_ctz(~alloc);
#endif

        keys[idx] = p->key[p->off++];
        p->slot = radix + idx;
        p->fillKey();
        alloc |= 1 << idx;
        return EndSearch;
    }

    // the radix node is full, promote to the next larger size.
    // mark all the keys as currently unused.

    ARTAddr addr = p->art->arenaAllocNode(p->set, Array64);
    ARTnode64* radix64Node = static_cast<ARTnode64*>(p->art->arenaSlotAddr(addr));

    memset((void*)radix64Node->keys, 0xff, sizeof(radix64Node->keys));

    p->newSlot->bits = 0;
    p->newSlot->off = addr.off;
    p->newSlot->type = Array64;

    for (idx = 0; idx < 14; idx++) {
        ARTSlot* slot = radix + idx;
        MutexSpinLock::lock(slot);
        if (!slot->dead) {
            uint32_t out;

// TODO: move this to artree_util, use platform/bits.h abstraction
#ifdef WIN32
            _BitScanForward64((DWORD*)&out, ~radix64Node->alloc);
#else
            out = __builtin_ctzl(~radix64Node->alloc);
#endif

            radix64Node->alloc |= 1ULL << out;
            radix64Node->radix[out].bits = slot->bits;  // copies mutex
            radix64Node->keys[keys[idx]] = out;
            radix64Node->radix[out].mutex = 0;
            MutexSpinLock::kill(slot);
        }
        MutexSpinLock::unlock(slot);
    }

    uint32_t out;

// TODO: move this to artree_util, use platform/bits.h abstraction
#ifdef WIN32
    _BitScanForward64((DWORD*)&out, ~radix64Node->alloc);
#else
    out = __builtin_ctzl(~radix64Node->alloc);
#endif

    radix64Node->keys[p->key[p->off++]] = out;

    // fill in rest of the key bytes into span nodes
    p->slot = radix64Node->radix + out;
    p->fillKey();
    radix64Node->alloc |= 1ULL << out;
    return EndSearch;
}

ReturnState ARTnode64::insertKey(ParamStruct* p) {
    uint32_t idx = keys[p->key[p->off]];

    if (idx < 0xff && alloc & (1ULL << idx)) {
        p->slot = radix + idx;
        p->off++;
        return ContinueSearch;
    }

    // obtain write lock on the node
    MutexSpinLock::lock(p->slot);

    // restart if slot has been killed
    // or node has changed.
    if (p->slot->dead) {
        MutexSpinLock::unlock(p->slot);
        return RestartSearch;
    }

    if (p->slot->off != p->newSlot->off) {
        MutexSpinLock::unlock(p->slot);
        return RetrySearch;
    }

    //  retry under lock
    idx = keys[p->key[p->off]];
    if (idx < 0xff && alloc & (1ULL << idx)) {
        MutexSpinLock::unlock(p->slot);
        p->slot = radix + idx;
        p->off++;
        return ContinueSearch;
    }

    // if room, add to radix node
    if (alloc < std::numeric_limits<uint64_t>::max()) {
        idx = p->key[p->off++];
        uint32_t out;

// TODO: move this to artree_util, use platform/bits.h abstraction
#ifdef WIN32
        _BitScanForward64((DWORD*)&out, ~alloc);
#else
        out = __builtin_ctzl(~alloc);
#endif

        p->slot = radix + out;
        p->fillKey();
        keys[idx] = out;
        alloc |= 1ULL << out;
        return EndSearch;
    }

    // the radix node is full, promote to the next larger size.
    ARTAddr addr = p->art->arenaAllocNode(p->set, Array256);
    ARTnode256* radix256Node = static_cast<ARTnode256*>(p->art->arenaSlotAddr(addr));

    p->newSlot->bits = 0;
    p->newSlot->type = Array256;
    p->newSlot->off = addr.off;

    for (idx = 0; idx < 256; idx++) {
        if (keys[idx] < 0xff)
            if (alloc & (1ULL << keys[idx])) {
                ARTSlot* slot = radix + keys[idx];
                MutexSpinLock::lock(slot);
                if (!slot->dead) {
                    radix256Node->alloc[idx / 64] |= 1ULL << (idx % 64);
                    radix256Node->radix[idx].bits = slot->bits;  // copies mutex
                    radix256Node->radix[idx].mutex = 0;
                    MutexSpinLock::kill(slot);
                }
                MutexSpinLock::unlock(slot);
            }
    }

    // fill in the rest of the key bytes into Span nodes
    idx = p->key[p->off++];
    p->slot = radix256Node->radix + idx;
    p->fillKey();
    radix256Node->alloc[idx / 64] |= 1ULL << (idx % 64);
    return EndSearch;
}

ReturnState ARTnode256::insertKey(ParamStruct* p) {
    uint32_t idx = p->key[p->off];

    if (alloc[idx / 64] & (1ULL << (idx % 64))) {
        p->slot = radix + idx;
        p->off++;
        return ContinueSearch;
    }

    // obtain write lock on the node
    MutexSpinLock::lock(p->slot);

    // restart if slot has been killed
    // or node has changed.
    if (p->slot->dead) {
        MutexSpinLock::unlock(p->slot);
        return RestartSearch;
    }

    if (p->slot->off != p->newSlot->off) {
        MutexSpinLock::unlock(p->slot);
        return RetrySearch;
    }

    //  retry under lock
    if (alloc[idx / 64] & (1ULL << (idx % 64))) {
        MutexSpinLock::unlock(p->slot);
        p->slot = radix + idx;
        p->off++;
        return ContinueSearch;
    }

    p->off++;
    p->slot = radix + idx;
    p->fillKey();
    alloc[idx / 64] |= 1ULL << (idx % 64);
    return EndSearch;
}

ReturnState ARTspan::insertKey(ParamStruct* p) {
    uint32_t len = p->newSlot->nbyte;
    uint32_t max = len;
    uint32_t idx;

    if (len > p->keylen - p->off)
        len = p->keylen - p->off;

    for (idx = 0; idx < len; idx++)
        if (p->key[p->off + idx] != bytes[idx])
            break;

    // did we use the entire span node exactly?
    if (idx == max) {
        p->slot = next;
        p->off += idx;
        return ContinueSearch;
    }

    // obtain write lock on the node
    MutexSpinLock::lock(p->slot);

    // restart if slot has been killed
    // or node has changed.
    if (p->slot->dead) {
        MutexSpinLock::unlock(p->slot);
        return RestartSearch;
    }

    if (p->slot->off != p->newSlot->off) {
        MutexSpinLock::unlock(p->slot);
        return RetrySearch;
    }

    MutexSpinLock::lock(next);

    if (next->dead) {
        MutexSpinLock::unlock(p->slot);
        MutexSpinLock::unlock(next);
        return RestartSearch;
    }

    ARTSlot* contSlot = nullptr;
    ARTSlot* nxtSlot = nullptr;
    p->off += idx;

    // copy matching prefix bytes to a new span node
    if (idx) {
        ARTAddr addr = p->art->arenaAllocNode(p->set, SpanNode);
        ARTspan* spanNode2 = static_cast<ARTspan*>(p->art->arenaSlotAddr(addr));
        memcpy(spanNode2->bytes, bytes, idx);
        p->newSlot->bits = 0;
        p->newSlot->off = addr.off;
        p->newSlot->type = SpanNode;
        p->newSlot->nbyte = idx;
        nxtSlot = spanNode2->next;
        contSlot = nxtSlot;
    } else {
        // else change the original node to a radix4 node.
        // note that p->off < p->keylen, which will set contSlot
        nxtSlot = p->newSlot;
    }

    // if we have more key bytes, insert a radix node after span1 and before
    // possible
    // span2 for the next key byte and the next remaining original span byte (if
    // any).
    // note:  max > idx
    if (p->off < p->keylen) {
        ARTAddr addr = p->art->arenaAllocNode(p->set, Array4);
        ARTnode4* radix4Node = static_cast<ARTnode4*>(p->art->arenaSlotAddr(addr));
        nxtSlot->bits = 0;
        nxtSlot->off = addr.off;
        nxtSlot->type = Array4;

        // fill in first radix element with first of the remaining span bytes
        radix4Node->keys[0] = bytes[idx++];
        radix4Node->alloc |= 1;
        nxtSlot = radix4Node->radix + 0;

        // fill in second radix element with next byte of our search key
        radix4Node->keys[1] = p->key[p->off++];
        radix4Node->alloc |= 2;
        contSlot = radix4Node->radix + 1;
    }

    // place original span bytes remaining after the preceeding node
    // in a second span node after the radix or span node
    // i.e. fill in nxtSlot.

    if (max - idx) {
        ARTAddr addr = p->art->arenaAllocNode(p->set, SpanNode);
        ARTspan* overflowSpanNode = static_cast<ARTspan*>(p->art->arenaSlotAddr(addr));
        memcpy(overflowSpanNode->bytes, bytes + idx, max - idx);
        overflowSpanNode->next->bits = next->bits;
        overflowSpanNode->next->mutex = 0;

        // append second span node after span or radix node from above
        nxtSlot->bits = 0;
        nxtSlot->off = addr.off;
        nxtSlot->nbyte = max - idx;
        nxtSlot->type = SpanNode;

    } else {
        // otherwise hook remainder of the trie into the
        // span or radix node's next slot (nxtSlot)
        nxtSlot->bits = next->bits;
        nxtSlot->mutex = 0;
    }

    MutexSpinLock::kill(next);
    MutexSpinLock::unlock(next);

    // fill in the rest of the key into the radix and end the insert.
    p->slot = contSlot;

    if (p->off < p->keylen)
        p->fillKey();

    return EndSearch;
}

std::ostream& operator<<(std::ostream& os, const ParamStruct& s) {
    os << "ParamStruct[\n  key = ";
    for (uint32_t i = 0; i < s.keylen; i++) {
        if (i > 0)
            os << ',';
        os << ARTree::toHexByte(static_cast<uint8_t>(s.key[i]));
    }
    os << "\n  off = " << s.off << "\n  set = " << s.set << std::endl;
    return os << "]";
}

// fill in the empty slot with remaining key bytes

void ParamStruct::fillKey() {
    uint32_t len;
    slot->bits = 0;

    while ((len = (keylen - off))) {
        ARTAddr addr = art->arenaAllocNode(set, SpanNode);
        ARTspan* spanNode = static_cast<ARTspan*>(art->arenaSlotAddr(addr));

        if (len > sizeof(spanNode->bytes)) {
            len = sizeof(spanNode->bytes);
        }

        memcpy(spanNode->bytes, key + off, len);
        slot->off = addr.off;
        slot->type = SpanNode;
        slot->nbyte = len;

        slot = spanNode->next;
        off += len;
    }
}

}  // namespace mongo
