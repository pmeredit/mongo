//@file artree_synchronous_queue.h
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

#include "mongo/platform/basic.h"

#include "artree_ordered_list.h"
#include "artree_util.h"

namespace mongo {

void ARTreeOrderedList::insert(Entry* entry, uint64_t value) {
    MutexSpinLock::lock(_mutex);
    entry->_value = value;
    entry->_next = 0;

    if ((entry->_prev = _tail)) {
        Entry* prev = _tail;
        prev->_next = entry;
    } else {
        _minimum = entry->_value;
        _head = entry;
    }

    _tail = entry;
    MutexSpinLock::unlock(_mutex);
}

void ARTreeOrderedList::clear() {
    for (Entry* entry = _tail; entry; entry = entry->_prev)
        entry->_value = 0;
}

void ARTreeOrderedList::remove(Entry* entry) {
    MutexSpinLock::lock(_mutex);
    Entry* prev = entry->_prev;
    Entry* next = entry->_next;

    if (prev) {
        prev->_next = next;
    } else if (next) {
        _head = next;
        _minimum = next->_value;
    } else {
        _minimum = 0;
    }

    if (next)
        next->_prev = prev;
    else
        _tail = prev;

    entry = {0};
    MutexSpinLock::unlock(_mutex);
}

}  // namespace mongo
