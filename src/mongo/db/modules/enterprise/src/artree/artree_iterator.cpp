//@file artree_iterator.cpp
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "artree.h"
#include "artree_iterator.h"
#include "artree_records.h"
#include "artree_common.h"
#include "artree_nodes.h"
#include "artree_recovery_unit.h"
#include "artree_util.h"
#include "artree_debug.h"

#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/log.h"

namespace mongo {

void ARTreeIterator::init(OperationContext* opCtx, ARTree* art, bool isForward) {
    _opCtx = opCtx;
    _art = art;
    _isForward = isForward;
    _isInited = false;
    _countIt = 0;
    _eofMax = false;
    _eofMin = false;

    _prevId.off = 0;
    _recordId.off = 0;

    ARTreeRecoveryUnit* recUnit = ARTree::recUnit(opCtx);
    _entry[0] = {0};
    _art->_cursors->insert(_entry, recUnit->getTxnId());
}

void ARTreeIterator::deinit() {
    if (_entry->_value)
        _art->_cursors->remove(_entry);
}

void ARTreeIterator::start() {
    if (_isInited)
        return;

    _eofMax = false;
    _eofMin = false;
    _isInited = true;

    if (_isForward) {
        if (_art->isCapped())
            _recordId.off = _art->_cappedBase;
        else
            _recordId.off = 0;
    } else {
        if (_art->_uncommittedRecs->getMinimum()) {
            _recordId.off = _art->_uncommittedRecs->getMinimum();
        } else {
            if (_art->isCapped())
                _recordId.off = _art->_cappedRec;
            else
                _recordId = _art->_arena[_art->_currentArena]->_arenaRec;
            _recordId._offset++;
        }
    }
}

void ARTreeIterator::setStart() {
    _isInited = false;
}

// next() = it++
uint64_t ARTreeIterator::next(bool visible) {
    if (FUNCTION_TRACE)
        log() << "ARTreeIterator::" << __FUNCTION__ << ":" << __LINE__;

    _eofMax = false;
    bool doDecr;

    while ((doDecr = _art->incr(&_recordId))) {
        ARTRecord* rec = _art->fetchRec(_recordId);

        // capped visibility
        if (_art->_cappedCollection) {
            if (isDead())
                break;

            if (!rec || !rec->_used || !rec->_basever)
                break;

            uint64_t ts = rec->_deadstamp;

            if (ts) {
                if (ts > _entry->_value)
                    break;
                else
                    continue;
            }

            ts = rec->_timestamp;

            // check reader ts, or write ts later than cursor inception
            // TODO: check if we can remove this code
            if ((ts & 1) == 0 || ts > _entry->_value)
                if (!visible || !rec->_opCtx || rec->_opCtx != _opCtx)
                    break;

            return _recordId.off;
        }

        if (!rec || !rec->_used || !rec->_basever)
            continue;

        ARTAddr recId = _art->followVersionChain(_opCtx, _entry->_value, _recordId);
        if (recId.off)
            return recId.off;

        // TODO: can we replace the previous three lines with the following?
        //if (_art->isVisibleVersion(_opCtx, _entry->_value, _recordId))
        //    return _recordId.off;
    }

    setEof(true);
    if (doDecr)
        _art->decr(&_recordId);
    return _recordId.off;
}

// prev() = it--
uint64_t ARTreeIterator::prev(bool visible) {
    _eofMin = false;
    bool doIncr;

    while ((doIncr = _art->decr(&_recordId))) {
        ARTRecord* rec = _art->fetchRec(_recordId);

        // capped visibility
        if (_art->_cappedCollection) {
            if (!rec || !rec->_used || !rec->_basever)
                continue;  // note: spec says that reverse cursors
                           // do not stall, returning either the
                           // first visible record or eof.

            uint64_t ts = rec->_deadstamp;

            if (ts) {
                if (ts > _entry->_value)
                    break;
                else
                    continue;
            }

            ts = rec->_timestamp;

            // check reader ts, or write ts later than cursor inception
            // TODO: check if we can remove this code
            if ((ts & 1) == 0 || ts > _entry->_value)
                if (!visible || !rec->_opCtx || rec->_opCtx != _opCtx)
                    continue;

            return _recordId.off;
        }

        if (!rec || !rec->_used || !rec->_basever)
            continue;

        ARTAddr recId = _art->followVersionChain(_opCtx, _entry->_value, _recordId);
        if (recId.off)
           return recId.off;

        // TODO: can we replace the previous three lines with the following?
        //if (_art->isVisibleVersion(_opCtx, _entry->_value, _recordId))
        //    return _recordId.off;
    }

    setEof(true);
    if (doIncr)
        _art->incr(&_recordId);
    return _recordId.off;
}

uint64_t ARTreeIterator::setRec(uint64_t loc) {
    _recordId.off = loc;
    _isInited = true;

    if (_art->_cappedCollection && !_art->_cappedRec) {
        _eofMax = true;
        _eofMin = true;
        return 0;
    }
    if (!_art->_cappedCollection && !_art->_arena[0]->_arenaRec.off) {
        _eofMax = true;
        _eofMin = true;
        return 0;
    }

    _eofMax = false;
    _eofMin = false;

    if (_isForward) {
        _recordId._offset--;
        next(true);
    } else {
        _recordId._offset++;
        prev(true);
    }

    if (!eof())
        return _recordId.off;

    _eofMax = false;
    _eofMin = false;
    return 0;
}

bool ARTreeIterator::eof() {
    return (_isForward ? _eofMax : _eofMin);
}

void ARTreeIterator::setEof(bool b) {
    if (_isForward)
        _eofMax = b;
    else
        _eofMin = b;
}

void ARTreeIterator::saveState() {
    _prevId = _recordId;
}

bool ARTreeIterator::restoreState() {
    // reset the timestamp
    // move cursor to the front of the queue
    if (_entry->_value)
        _art->dequeueCursor(_entry);
    _art->enqueueCursor(_entry);

    _recordId = _prevId;
    return !isDead();
}

}  // namespace mongo
