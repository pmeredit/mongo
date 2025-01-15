/**
 *    Copyright (C) 2025-present MongoDB, Inc.
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

#include <cstdint>
#include <list>
#include <string>
#include <wiredtiger.h>

#include "mongo/db/storage/wiredtiger/wiredtiger_compiled_configuration.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/unordered_set.h"
#include "mongo/util/time_support.h"

namespace mongo {

class WiredTigerConnection;

/**
 * This is a structure that caches 1 cursor for each uri.
 * The idea is that there is a pool of these somewhere.
 * NOT THREADSAFE
 */
class WiredTigerSession {
public:
    /**
     * Creates a new WT session on the specified connection.
     *
     * @param conn WT connection
     */
    WiredTigerSession(WT_CONNECTION* conn);

    /**
     * Creates a new WT session on the specified connection.
     *
     * @param conn WT connection
     * @param connection The WiredTigerConnection that owns this session.
     * @param epoch In which session cache cleanup epoch was this session instantiated.
     */
    WiredTigerSession(WT_CONNECTION* conn, WiredTigerConnection* connection, uint64_t epoch = 0);

    /**
     * Creates a new WT session on the specified connection.
     *
     * @param conn WT connection
     * @param handler Callback handler that will be invoked by wiredtiger.
     * @param config configuration string used to open the session with.
     */
    WiredTigerSession(WT_CONNECTION* conn, WT_EVENT_HANDLER* handler, const char* config);

    ~WiredTigerSession();

    // TODO(SERVER-98126): Remove these 3 ways of directly-accessing the session.

    WT_SESSION* getSession() const {
        return _session;
    }
    WT_SESSION* operator*() const {
        return _session;
    }
    WT_SESSION* operator->() const {
        return _session;
    }

    // Safe accessor for the internal session
    template <typename Functor>
    auto with(Functor functor) {
        stdx::lock_guard<stdx::mutex> lock(_sessionGuard);
        return functor(_session);
    }

#define WRAPPED_WT_SESSION_METHOD(name)                               \
    template <typename... Args>                                       \
    auto name(Args&&... args) {                                       \
        stdx::lock_guard<stdx::mutex> lock(_sessionGuard);            \
        return _session->name(_session, std::forward<Args>(args)...); \
    }

    WRAPPED_WT_SESSION_METHOD(compact)
    WRAPPED_WT_SESSION_METHOD(get_rollback_reason)
    WRAPPED_WT_SESSION_METHOD(reconfigure)
#undef WRAPPED_WT_SESSION_METHOD

    /**
     * Gets a cursor on the table id 'id' with optional configuration, 'config'.
     *
     * This may return a cursor from the cursor cache and these cursors should *always* be released
     * into the cache by calling releaseCursor().
     */
    WT_CURSOR* getCachedCursor(uint64_t id, const std::string& config);


    /**
     * Create a new cursor and ignore the cache.
     *
     * The config string specifies optional arguments for the cursor. For example, when
     * the config contains 'read_once=true', this is intended for operations that will be
     * sequentially scanning large amounts of data.
     *
     * This will never return a cursor from the cursor cache, and these cursors should *never* be
     * released into the cache by calling releaseCursor(). Use closeCursor() instead.
     */
    WT_CURSOR* getNewCursor(const std::string& uri, const char* config);

    /**
     * Wrapper for getNewCursor() without a config string.
     */
    WT_CURSOR* getNewCursor(const std::string& uri) {
        return getNewCursor(uri, nullptr);
    }

    /**
     * Release a cursor into the cursor cache and close old cursors if the number of cursors in the
     * cache exceeds wiredTigerCursorCacheSize.
     * The exact cursor config that was used to create the cursor must be provided or subsequent
     * users will retrieve cursors with incorrect configurations.
     *
     * Additionally calls into the WiredTigerKVEngine to see if the SizeStorer needs to be flushed.
     * The SizeStorer gets flushed on a periodic basis.
     */
    void releaseCursor(uint64_t id, WT_CURSOR* cursor, std::string config);

    /**
     * Close a cursor without releasing it into the cursor cache.
     */
    void closeCursor(WT_CURSOR* cursor);

    /**
     * Closes all cached cursors matching the uri.  If the uri is empty,
     * all cached cursors are closed.
     */
    void closeAllCursors(const std::string& uri);

    int cursorsOut() const {
        return _cursorsOut;
    }

    int cachedCursors() const {
        return _cursors.size();
    }

    void setIdleExpireTime(Date_t idleExpireTime) {
        _idleExpireTime = idleExpireTime;
    }

    Date_t getIdleExpireTime() const {
        return _idleExpireTime;
    }

    void setCompiledConfigurationsPerConnection(CompiledConfigurationsPerConnection* compiled) {
        _compiled = compiled;
    }

    CompiledConfigurationsPerConnection* getCompiledConfigurationsPerConnection() {
        return _compiled;
    }

    /**
     * Reconfigures the session. Stores the config string that undoes this change when
     * resetSessionConfiguration() is called.
     */
    void modifyConfiguration(const std::string& newConfig, std::string undoConfig);

    /**
     * Reset the configurations for this session to the default. This should be done before we
     * release this session back into the session cache, so that any recovery unit that may use this
     * session in the future knows that the session will have the default configuration.
     */
    void resetSessionConfiguration();

    stdx::unordered_set<std::string> getUndoConfigStrings() {
        return _undoConfigStrings;
    }

    // Drops this session immediately (without calling close()). This may be necessary during
    // shutdown to avoid racing against the connection's close. Only call this method if you're
    // about to delete the session.
    void dropSessionBeforeDeleting() {
        invariant(_session);
        _session = nullptr;
    }

private:
    class CachedCursor {
    public:
        CachedCursor(uint64_t id, uint64_t gen, WT_CURSOR* cursor, std::string config)
            : _id(id), _gen(gen), _cursor(cursor), _config(std::move(config)) {}

        uint64_t _id;   // Source ID, assigned to each URI
        uint64_t _gen;  // Generation, used to age out old cursors
        WT_CURSOR* _cursor;
        std::string _config;  // Cursor config. Do not serve cursors with different configurations
    };

    friend class WiredTigerConnection;

    // The cursor cache is a list of pairs that contain an ID and cursor
    typedef std::list<CachedCursor> CursorCache;

    // Used internally by WiredTigerConnection
    uint64_t _getEpoch() const {
        return _epoch;
    }

    const uint64_t _epoch;

    // This protects against concurrent calls into the WiredTiger API through this session (i.e. it
    // must be locked for uses of the session, or any cursor created from it).
    stdx::mutex _sessionGuard;
    WT_SESSION* _session;  // owned
    CursorCache _cursors;  // owned
    uint64_t _cursorGen;
    int _cursorsOut;

    WiredTigerConnection* _conn;                     // not owned
    CompiledConfigurationsPerConnection* _compiled;  // not owned

    Date_t _idleExpireTime;

    // A set that contains the undo config strings for any reconfigurations we might have performed
    // on a session during the lifetime of this recovery unit. We use these to reset the session to
    // its default configuration before returning it to the session cache.
    stdx::unordered_set<std::string> _undoConfigStrings;
};

}  // namespace mongo
