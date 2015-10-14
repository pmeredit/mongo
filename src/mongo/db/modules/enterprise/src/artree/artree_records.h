//@file artree_records.h
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

#pragma once

#include <atomic>

#include "artree_common.h"
#include "artree_nodes.h"
#include "artree_ordered_list.h"

namespace mongo {

// records are allocated from an array at the top end of the Arena
// the recordID is the index into this array

class ARTreeIndex;

class ARTRecord {
public:
    uint64_t _timestamp;                 // commit timestamp from global clock
    uint64_t _deadstamp;                 // delete timestamp from global clock
    uint64_t _version;                   // counter since beginning of time
    ARTreeOrderedList::Entry _queue[1];  // capped collection open transactions
    ARTAddr _prevVersion;                // older version of the record (if any)
    ARTAddr _base;                       // base version of the record
    ARTSlot _doc[1];                     // pointer to the document
    ARTSlot _oldDoc[1];                  // pointer to the original document
    void* _lastCtx;                      // operation context for committed txn
    void* _opCtx;                        // operation context for uncommitted txn's
    char _mutex[1];                      // record is locked
    bool _basever;                       // slot is a base version
    bool _used;                          // slot is in use

    void reset();  // unset all fields
};

class ARTDocument {
public:
    uint32_t _docLen;      // length of the document in bytes
    uint8_t _document[0];  // array of the document bytes follows.

    const char* doc() const {
        return reinterpret_cast<const char*>(_document);
    }
    uint32_t docLen() const {
        return _docLen;
    }
};

}  // namespace mongo
