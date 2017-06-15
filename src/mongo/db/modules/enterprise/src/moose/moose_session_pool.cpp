/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "moose_session_pool.h"

#include <string>
#include <vector>

#include "../third_party/sqlite/sqlite3.h"
#include "mongo/db/client.h"
#include "mongo/db/operation_context.h"
#include "mongo/stdx/mutex.h"
#include "moose_session.h"
#include "moose_util.h"

namespace mongo {

MooseSessionPool::MooseSessionPool(const std::string& path, std::uint64_t maxPoolSize)
    : _path(path), _maxPoolSize(maxPoolSize) {}

MooseSessionPool::~MooseSessionPool() {
    shutDown();
}

std::unique_ptr<MooseSession> MooseSessionPool::getSession(OperationContext* opCtx) {
    stdx::unique_lock<stdx::mutex> lk(_mutex);

    // We should never be able to get here after _shuttingDown is set, because no new operations
    // should be allowed to start.
    invariant(!_shuttingDown);

    // Checks if there is an open session available.
    if (!_sessions.empty()) {
        sqlite3* session = _popSession_inlock();
        return stdx::make_unique<MooseSession>(session, this);
    }

    // Checks if a new session can be opened.
    if (_curPoolSize < _maxPoolSize) {
        sqlite3* session;
        int status = sqlite3_open(_path.c_str(), &session);
        checkStatus(status, SQLITE_OK, "sqlite3_open");
        _curPoolSize++;
        return stdx::make_unique<MooseSession>(session, this);
    }

    // There are no open sessions available and the maxPoolSize has been reached.
    // waitForConditionOrInterrupt is notified when an open session is released and available.
    opCtx->waitForConditionOrInterrupt(
        _releasedSessionNotifier, lk, [&] { return !_sessions.empty(); });

    sqlite3* session = _popSession_inlock();
    return stdx::make_unique<MooseSession>(session, this);
}

void MooseSessionPool::releaseSession(MooseSession* session) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _sessions.push_back(session->getSession());
    _releasedSessionNotifier.notify_one();
}

void MooseSessionPool::shutDown() {
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    _shuttingDown = true;

    // Retrieve the operation context from the thread's client if the client exists.
    if (haveClient()) {
        OperationContext* opCtx = cc().getOperationContext();

        // Locks if the operation context still exists.
        if (opCtx) {
            opCtx->waitForConditionOrInterrupt(
                _releasedSessionNotifier, lk, [&] { return _sessions.size() == _curPoolSize; });
        }
    } else {
        _releasedSessionNotifier.wait(lk, [&] { return _sessions.size() == _curPoolSize; });
    }

    for (auto&& session : _sessions) {
        sqlite3_close(session);
    }
}

// This method should only be called when _sessions is locked.
sqlite3* MooseSessionPool::_popSession_inlock() {
    sqlite3* session = _sessions.back();
    _sessions.pop_back();
    return session;
}

}  // namespace mongo
