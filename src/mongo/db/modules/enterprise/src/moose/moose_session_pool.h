/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#pragma once

#include <string>
#include <vector>

#include "../third_party/sqlite/sqlite3.h"
#include "mongo/base/disallow_copying.h"
#include "mongo/db/operation_context.h"
#include "mongo/stdx/mutex.h"
#include "moose_session.h"

namespace mongo {
class MooseSession;

/**
 * This class manages a pool of open sqlite3* objects.
 */
class MooseSessionPool final {
    MONGO_DISALLOW_COPYING(MooseSessionPool);

public:
    MooseSessionPool(const std::string& path, std::uint64_t maxPoolSize = 80);

    ~MooseSessionPool();

    /**
     * Returns a smart pointer to a previously released session for reuse, or creates a new session.
     */
    std::unique_ptr<MooseSession> getSession(OperationContext* opCtx);

    /**
     * Returns a session to the pool for later reuse.
     */
    void releaseSession(MooseSession* session);

    /**
     * Transitions the pool to shutting down mode. It waits until all sessions are released back
     * into the pool and closes all open sessions.
     */
    void shutDown();

private:
    /**
     * Gets the front element from _sessions and then pops it off the queue.
     */
    sqlite3* _popSession_inlock();

    // This is used to lock the _sessions vector.
    stdx::mutex _mutex;
    stdx::condition_variable _releasedSessionNotifier;

    std::string _path;

    /**
     * PoolSize is the number of open sessions associated with the session pool.
     */
    std::uint64_t _maxPoolSize = 80;
    std::uint64_t _curPoolSize = 0;
    bool _shuttingDown = false;

    using SessionPool = std::vector<sqlite3*>;
    SessionPool _sessions;
};
}  // namespace mongo
