/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <deque>
#include <map>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/cursor_id.h"
#include "mongo/db/service_context.h"
#include "mongo/stdx/mutex.h"

namespace mongo {
namespace mongotmock {
struct ExpectedCommandResponsePair {
    BSONObj expectedCommand;
    BSONObj response;
};

class CursorState final {
public:
    CursorState(std::deque<ExpectedCommandResponsePair> cmdResponsePairs)
        : _remainingResponses(std::move(cmdResponsePairs)) {}

    bool claimed() const {
        return _claimed;
    }

    bool hasNextCursorResponse() {
        return !_remainingResponses.empty();
    }

    ExpectedCommandResponsePair peekNextCommandResponsePair() {
        invariant(hasNextCursorResponse());
        return _remainingResponses.front();
    }

    void popNextCommandResponsePair() {
        invariant(hasNextCursorResponse());
        _remainingResponses.pop_front();
    }

    const std::deque<ExpectedCommandResponsePair>& getRemainingResponses() {
        return _remainingResponses;
    }

private:
    void claim() {
        invariant(!_claimed);
        _claimed = true;
    }

    std::deque<ExpectedCommandResponsePair> _remainingResponses;

    // Whether some client is already using/iterating this state.
    bool _claimed = false;

    // MongotMockState is a friend so that it may call claim() on a CursorState.
    friend class MongotMockState;
};

class MongotMockStateGuard;
class MongotMockState final {
public:
    using CursorMap = std::map<CursorId, std::unique_ptr<CursorState>>;

    void setStateForId(CursorId id, std::unique_ptr<CursorState> state) {
        auto it = _cursorStates.find(id);
        if (it == _cursorStates.end()) {
            // No existing state. Just insert and we're done.
            _availableCursorIds.push_back(id);
            _cursorStates.insert(CursorMap::value_type(id, std::move(state)));
        } else {
            if (!it->second->claimed()) {
                // There is an existing unclaimed state. We must remove it from the list of
                // available cursors. Later, we will re-insert it to the end of the list.
                auto it = std::find(_availableCursorIds.begin(), _availableCursorIds.end(), id);
                invariant(it != _availableCursorIds.end());
                _availableCursorIds.erase(it);
            }

            // Now we are guaranteed that there is no entry for this cursor in _availableCursorIds.
            _availableCursorIds.push_back(id);
            _cursorStates.insert_or_assign(id, std::move(state));
        }
    }

    CursorState* claimAvailableState() {
        if (_availableCursorIds.empty()) {
            return nullptr;
        }
        CursorId id = _availableCursorIds.front();
        _availableCursorIds.pop_front();

        auto it = _cursorStates.find(id);
        invariant(it != _cursorStates.end());
        it->second->claim();
        return it->second.get();
    }

    const CursorMap& getCursorMap() {
        return _cursorStates;
    }

    CursorState* getCursorState(CursorId id) {
        auto it = _cursorStates.find(id);
        if (it == _cursorStates.end()) {
            return nullptr;
        }
        return it->second.get();
    }

private:
    // List of unused cursor ids ordered by insertion time (oldest to newest).
    std::list<CursorId> _availableCursorIds;
    CursorMap _cursorStates;

    // Protects access to all members. Should be acquired using a MongotMockStateGuard.
    stdx::mutex _lock;

    friend class MongotMockStateGuard;
};

class MongotMockStateGuard final {
public:
    MongotMockStateGuard(MongotMockState* s) : lk(s->_lock), state(s) {}

    MongotMockState* operator->() const {
        return state;
    }

private:
    stdx::lock_guard<stdx::mutex> lk;
    MongotMockState* state;
};

/**
 * Provides access to a service context scoped mock state.
 */
MongotMockStateGuard getMongotMockState(ServiceContext* svc);
}  // namespace mongotmock
}  // namespace mongo
