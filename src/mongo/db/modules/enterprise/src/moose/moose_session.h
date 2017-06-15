/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#pragma once

#include <string>

#include "../third_party/sqlite/sqlite3.h"
#include "mongo/base/disallow_copying.h"
#include "moose_session_pool.h"

namespace mongo {
class MooseSessionPool;

/**
 * This class manages a SQLite database connection object.
 */
class MooseSession final {
    MONGO_DISALLOW_COPYING(MooseSession);

public:
    MooseSession(sqlite3* session, MooseSessionPool* sessionPool);

    ~MooseSession();

    /**
     * Returns a pointer to the underlying SQLite connection object.
     */
    sqlite3* getSession() const;

private:
    sqlite3* _session;
    MooseSessionPool* _sessionPool;
};
}  // namespace mongo
