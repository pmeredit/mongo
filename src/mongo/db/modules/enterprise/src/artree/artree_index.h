//@file artree_index.h
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
// Some of this is derived code.  The original code is in the public domain.
//    ARTful5: Adaptive Radix Trie key-value store
//    Author: Karl Malbrain, malbrain@cal.berkeley.edu
//    Date:   13 JAN 15
//

#pragma once

#include "mongo/bson/ordering.h"

#include <atomic>
#include <iostream>
#include <stdlib.h>
#include <string.h>

#include "artree.h"
#include "artree_nodes.h"

namespace mongo {

class ARTreeIndex {
public:
    ARTreeIndex(ARTree* art) : _art(art) {
        _numEntries.store(0), _root->bits = 0;
    }

    /**
     *  check for duplicate key
     */
    bool hasDups(ARTSlot* leafSlot);

    /**
    *  find key in ART, returning pointer to value slot or nullptr
    */
    ARTSlot* findKey(ARTreeCursor* cursor,
                     ARTSlot* root,
                     const uint8_t* key,  // TODO: consider using StringData
                     uint32_t keylen);

    /**
     *  insert key into ARTree
     *  @return true,  if key inserted,
     *          false, if duplicate and not allowed.
     */
    bool insertDocKey(uint32_t set,
                      const uint8_t* key,
                      uint32_t keylen,
                      bool dupsAllowed,
                      uint64_t recId,
                      ARTSlot* docSlot);

    bool insertDocKey(
        uint32_t set, const uint8_t* key, uint32_t keylen, bool dupsAllowed, ARTSlot* docSlot);

    /**
     *  insert key/value into ART
     *  @return pointer to leaf value slot.
     */
    ARTSlot* insertKey(ARTSlot* root, uint32_t set, const uint8_t* key, uint32_t keylen);

    /**
     *  delete key in ART for a document
     *  @return true if key found and deleted, otherwise false
     */
    bool deleteDocKey(
        uint32_t set, const uint8_t* key, uint32_t keylen, uint64_t recId, ARTSlot* docSlot);

    bool deleteDocKey(
        uint32_t set, void* opCtx, const uint8_t* key, uint32_t keylen, ARTSlot* docSlot);

    uint64_t numEntries() const;

    Status getMetadata(OperationContext*, BSONObjBuilder*);

    ARTree* _art;
    std::atomic_uint_fast64_t _numEntries;
    ARTSlot _root[1];
};

}  // namespace mongo
