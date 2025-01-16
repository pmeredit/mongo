/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <vector>
#include <wiredtiger.h>

#include "mongo/db/storage/wiredtiger/wiredtiger_compiled_configuration.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_snapshot_manager.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/clock_source.h"
#include "mongo/util/interruptible.h"
#include "mongo/util/time_support.h"

namespace mongo {

class WiredTigerKVEngine;

/**
 *  This is a wrapper class for WT_CONNECTION and contains a shared pool of cached WiredTiger
 * sessions with the goal to amortize the cost of session creation and destruction over multiple
 * uses.
 */
class WiredTigerConnection {
public:
    using SessionId = int64_t;  // TODO SERVER-99352 use WiredTiger session id.

    WiredTigerConnection(WiredTigerKVEngine* engine);
    WiredTigerConnection(WT_CONNECTION* conn,
                         ClockSource* cs,
                         WiredTigerKVEngine* engine = nullptr);
    ~WiredTigerConnection();

    /**
     * This deleter automatically releases WiredTigerSession objects when no longer needed.
     */
    class WiredTigerSessionDeleter {
    public:
        void operator()(WiredTigerSession* session) const;
    };

    // RAII type to block and unblock the WiredTigerConnection to shut down.
    class BlockShutdown {
    public:
        BlockShutdown(WiredTigerConnection* connection) : _conn(connection) {
            _conn->_shuttingDown.fetchAndAdd(1);
        }

        ~BlockShutdown() {
            _conn->_shuttingDown.fetchAndSubtract(1);
        }

    private:
        WiredTigerConnection* _conn;
    };

    /**
     * Indicates that WiredTiger should be configured to cache cursors.
     */
    static bool isEngineCachingCursors();

    /**
     * Returns a smart pointer to a previously released session for reuse, or creates a new session.
     * This method must only be called while holding the global lock to avoid races with
     * shuttingDown, but otherwise is thread safe.
     */
    std::unique_ptr<WiredTigerSession, WiredTigerSessionDeleter> getSession();

    /**
     * Get a count of idle sessions in the session cache.
     */
    size_t getIdleSessionsCount();

    /**
     * Closes all cached sessions whose idle expiration time has been reached.
     */
    void closeExpiredIdleSessions(int64_t idleTimeMillis);

    /**
     * Free all cached sessions and ensures that previously acquired sessions will be freed on
     * release.
     */
    void closeAll();

    /**
     * Closes all cached cursors matching the uri.  If the uri is empty,
     * all cached cursors are closed.
     */
    void closeAllCursors(const std::string& uri);

    /**
     * Transitions the cache to shutting down mode. Any already released sessions are freed and
     * any sessions released subsequently are leaked. Must be called while holding the global
     * lock in exclusive mode to avoid races with getSession.
     */
    void shuttingDown();

    /**
     * True when in the process of shutting down.
     */
    bool isShuttingDown();

    /**
     * Restart a previously shut down cache.
     */
    void restart();

    bool isEphemeral();

    /**
     * Waits until a prepared unit of work has ended (either been commited or aborted). This
     * should be used when encountering WT_PREPARE_CONFLICT errors. The caller is required to retry
     * the conflicting WiredTiger API operation. A return from this function does not guarantee that
     * the conflicting transaction has ended, only that one prepared unit of work in the process has
     * signaled that it has ended.
     * Accepts an Interruptible that will throw an AssertionException when interrupted.
     *
     * This method is provided in WiredTigerConnection and not RecoveryUnit because all recovery
     * units share the same session cache, and we want a recovery unit on one thread to signal all
     * recovery units waiting for prepare conflicts across all other threads.
     */
    void waitUntilPreparedUnitOfWorkCommitsOrAborts(Interruptible& interruptible,
                                                    uint64_t lastCount);

    /**
     * Notifies waiters that the caller's prepared unit of work has ended (either committed or
     * aborted).
     */
    void notifyPreparedUnitOfWorkHasCommittedOrAborted();

    WT_CONNECTION* conn() const {
        return _conn;
    }

    WiredTigerSnapshotManager& snapshotManager() {
        return _snapshotManager;
    }
    const WiredTigerSnapshotManager& snapshotManager() const {
        return _snapshotManager;
    }

    WiredTigerKVEngine* getKVEngine() const {
        return _engine;
    }

    std::uint64_t getPrepareCommitOrAbortCount() const {
        return _prepareCommitOrAbortCounter.loadRelaxed();
    }

    CompiledConfigurationsPerConnection* getCompiledConfigurations() {
        return &_compiledConfigurations;
    }

    WiredTigerSession* getSessionById(const SessionId& id);

private:
    // TODO SERVER-99353 hook up the session registry.
    // Session registry.
    struct RegistryPartition {
        stdx::mutex mtx;
        stdx::unordered_map<SessionId, WiredTigerSession*> map;
    };

    void _addSession(const SessionId& id, WiredTigerSession* session);

    bool _removeSession(const SessionId& id);

    /**
     * Returns a session to the cache for later reuse. If closeAll was called between getting this
     * session and releasing it, the session is directly released. This method is thread safe.
     */
    void _releaseSession(WiredTigerSession* session);

    friend class WiredTigerSession;
    WT_CONNECTION* _conn;             // not owned
    ClockSource* const _clockSource;  // not owned
    WiredTigerKVEngine* _engine;      // not owned, might be NULL
    WiredTigerSnapshotManager _snapshotManager;
    CompiledConfigurationsPerConnection _compiledConfigurations;

    // Used as follows:
    //   The low 31 bits are a count of active calls that need to block shutdown.
    //   The high bit is a flag that is set if and only if we're shutting down.
    AtomicWord<unsigned> _shuttingDown{0};
    static const uint32_t kShuttingDownMask = 1 << 31;

    stdx::mutex _cacheLock;
    typedef std::vector<WiredTigerSession*> SessionCache;
    SessionCache _sessions;

    // Bumped when all open sessions need to be closed
    AtomicWord<unsigned long long> _epoch;  // atomic so we can check it outside of the lock

    // Mutex and cond var for waiting on prepare commit or abort.
    stdx::mutex _prepareCommittedOrAbortedMutex;
    stdx::condition_variable _prepareCommittedOrAbortedCond;
    AtomicWord<std::uint64_t> _prepareCommitOrAbortCounter{0};

    typedef std::vector<RegistryPartition> SessionRegistry;
    SessionRegistry _registry;
};

/**
 * A unique handle type for WiredTigerSession pointers obtained from a WiredTigerConnection.
 */
typedef std::unique_ptr<WiredTigerSession, typename WiredTigerConnection::WiredTigerSessionDeleter>
    UniqueWiredTigerSession;

static constexpr char kWTRepairMsg[] =
    "Please read the documentation for starting MongoDB with --repair here: "
    "http://dochub.mongodb.org/core/repair";
}  // namespace mongo
