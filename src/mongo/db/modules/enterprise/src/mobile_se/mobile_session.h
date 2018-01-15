/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#pragma once

#include <string>

#include "../third_party/sqlite/sqlite3.h"
#include "mobile_session_pool.h"
#include "mongo/base/disallow_copying.h"

namespace mongo {
class MobileSessionPool;

/**
 * This class manages a SQLite database connection object.
 */
class MobileSession final {
    MONGO_DISALLOW_COPYING(MobileSession);

public:
    MobileSession(sqlite3* session, MobileSessionPool* sessionPool);

    ~MobileSession();

    /**
     * Returns a pointer to the underlying SQLite connection object.
     */
    sqlite3* getSession() const;

private:
    sqlite3* _session;
    MobileSessionPool* _sessionPool;
};
}  // namespace mongo
