//@file artree_iterator.h
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

#include <iostream>
#include <sstream>

#include "artree_ordered_list.h"
#include "artree_common.h"
#include "artree.h"

#include "mongo/db/record_id.h"

namespace mongo {

class OperationContext;

/**
 *  the ART (Adaptive Radix Tree) record store iterator implementation
 */
class ARTreeIterator {
public:
    void init(OperationContext* opCtx, ARTree* art, bool isForward);
    void deinit();

    uint64_t next(bool visible);
    uint64_t prev(bool visible);
    uint64_t setRec(uint64_t loc);
    void start();

    bool isForward() const;
    bool eof();
    void setEof(bool b);

    bool isDead() const {
        if (!_art->isCapped())
            return false;
        if (_recordId.off <= _art->_cappedBase) {
            return true;
        }
        return false;
    }

    void saveState();
    bool restoreState();
    void setStart();

    RecordId getLoc() const {
        return RecordId(_recordId.off);
    }

    void setRecordId(uint64_t recId) {
        _recordId.off = recId;
        _eofMax = false;
        _eofMin = false;
    }

    friend class ARTreeRecordCursor;
    friend class ARTreeRecordReverseCursor;

private:
    ARTAddr _prevId;    // saved recordID
    ARTAddr _recordId;  // current recordID
    uint64_t _countIt;  // iteration counter
    ARTree* _art;       // parent
    OperationContext* _opCtx;
    ARTreeOrderedList::Entry _entry[1];
    bool _eofMax;     // 1 <=> cursor is past maximum recordId
    bool _eofMin;     // 1 <=> cursor is before minimum recordId
    bool _isForward;  // 1 <=> forward iterator
    bool _isInited;   // 1 <=> cursor seeked to beginning or end
};

}  // namespace mongo
