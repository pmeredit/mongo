//@file artree_ordered_list.h
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
#include <cstdint>

namespace mongo {

// priority queue members placed on stack, or in cursors, or recovery units.
// This data structure gets used to doubly link objects by priority in a
// thread-safe manner.  For example, all currently active cursor are linked
// together, with the youngest cursors at the head of the list. A save/restore
// will dequeue and re-enqueue the cursor to put it back at the head of the list.
//

class ARTreeOrderedList {
public:
    struct Entry {
        uint64_t _value;  // priority value
        Entry* _next;     // next higher member
        Entry* _prev;     // next lower number
    };

    ARTreeOrderedList() : _head(nullptr), _tail(nullptr) {}

    /**
     * insert entry at ordered position in doubly linked list
     */
    void insert(Entry* queue, uint64_t value);

    /**
     * remove entry from doubly linked list
     */
    void remove(Entry* queue);

    /**
     * clear values from all entries
     */
    void clear();

    uint64_t getMinimum() const {
        return _minimum;
    }

private:
    uint64_t _minimum;  // minimum value in the priority queue
    Entry* _head;       // head of the ordered list = current minimum value
    Entry* _tail;       // tail of the ordered list = current maximum value
    char _mutex[1];     // concurrency
};

}  // namespace mongo
