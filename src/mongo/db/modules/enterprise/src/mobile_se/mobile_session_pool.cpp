/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mobile_session_pool.h"

#include <queue>
#include <string>
#include <vector>

#include "../third_party/sqlite/sqlite3.h"
#include "mobile_session.h"
#include "mobile_sqlite_statement.h"
#include "mobile_util.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/operation_context.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/log.h"

namespace mongo {

MobileDelayedOpQueue::MobileDelayedOpQueue() : _isEmpty(true) {}

void MobileDelayedOpQueue::enqueueOp(std::string& opQuery) {
    _queueMutex.lock();
    // If the queue is empty till now, update the cached atomic to reflect the new state.
    if (_opQueryQueue.empty())
        _isEmpty.store(false);
    _opQueryQueue.push(opQuery);
    LOG(2) << "MobileSE: Enqueued operation for delayed execution: " << opQuery;
    _queueMutex.unlock();
}

void MobileDelayedOpQueue::execAndDequeueOp(MobileSession* session) {
    std::string opQuery;

    _queueMutex.lock();
    if (!_opQueryQueue.empty()) {
        opQuery = _opQueryQueue.front();
        _opQueryQueue.pop();
        // If the queue is empty now, set the cached atomic to reflect the new state.
        if (_opQueryQueue.empty())
            _isEmpty.store(true);
    }
    _queueMutex.unlock();

    LOG(2) << "MobileSE: Retrying previously enqueued operation: " << opQuery;
    try {
        SqliteStatement::execQuery(session, opQuery);
    } catch (const WriteConflictException&) {
        // It is possible that this operation fails because of a transaction running in parallel.
        // We re-enqueue it for now and keep retrying later.
        LOG(2) << "MobileSE: Caught WriteConflictException while executing previously enqueued "
                  "operation,  re-enquing it";
        enqueueOp(opQuery);
    }
}

void MobileDelayedOpQueue::execAndDequeueAllOps(MobileSession* session) {
    // Keep trying till the queue empties
    while (!_isEmpty.load())
        execAndDequeueOp(session);
}

bool MobileDelayedOpQueue::isEmpty() {
    return (_isEmpty.load());
}

MobileSessionPool::MobileSessionPool(const std::string& path, std::uint64_t maxPoolSize)
    : _path(path), _maxPoolSize(maxPoolSize) {}

MobileSessionPool::~MobileSessionPool() {
    shutDown();
}

std::unique_ptr<MobileSession> MobileSessionPool::getSession(OperationContext* opCtx) {
    stdx::unique_lock<stdx::mutex> lk(_mutex);

    // We should never be able to get here after _shuttingDown is set, because no new operations
    // should be allowed to start.
    invariant(!_shuttingDown);

    // Checks if there is an open session available.
    if (!_sessions.empty()) {
        sqlite3* session = _popSession_inlock();
        return stdx::make_unique<MobileSession>(session, this);
    }

    // Checks if a new session can be opened.
    if (_curPoolSize < _maxPoolSize) {
        sqlite3* session;
        int status = sqlite3_open(_path.c_str(), &session);
        checkStatus(status, SQLITE_OK, "sqlite3_open");
        _curPoolSize++;
        return stdx::make_unique<MobileSession>(session, this);
    }

    // There are no open sessions available and the maxPoolSize has been reached.
    // waitForConditionOrInterrupt is notified when an open session is released and available.
    opCtx->waitForConditionOrInterrupt(
        _releasedSessionNotifier, lk, [&] { return !_sessions.empty(); });

    sqlite3* session = _popSession_inlock();
    return stdx::make_unique<MobileSession>(session, this);
}

void MobileSessionPool::releaseSession(MobileSession* session) {
    // Retry drop that have been queued on failure
    if (!failedDropsQueue.isEmpty())
        failedDropsQueue.execAndDequeueOp(session);

    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _sessions.push_back(session->getSession());
    _releasedSessionNotifier.notify_one();
}

void MobileSessionPool::shutDown() {
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

    // Retry all the drops that have been queued on failure.
    // Create a new sqlite session to do so, all other sessions might have been closed already.
    if (!failedDropsQueue.isEmpty()) {
        sqlite3* session;

        int status = sqlite3_open(_path.c_str(), &session);
        checkStatus(status, SQLITE_OK, "sqlite3_open");
        std::unique_ptr<MobileSession> mobSession = stdx::make_unique<MobileSession>(session, this);
        LOG(2) << "MobileSE: Executing queued drops at shutdown";
        failedDropsQueue.execAndDequeueAllOps(mobSession.get());
        sqlite3_close(session);
    }

    for (auto&& session : _sessions) {
        sqlite3_close(session);
    }
}

// This method should only be called when _sessions is locked.
sqlite3* MobileSessionPool::_popSession_inlock() {
    sqlite3* session = _sessions.back();
    _sessions.pop_back();
    return session;
}

}  // namespace mongo
