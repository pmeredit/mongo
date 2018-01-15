/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mobile_session.h"

#include "../third_party/sqlite/sqlite3.h"
#include "mobile_session_pool.h"

namespace mongo {

MobileSession::MobileSession(sqlite3* session, MobileSessionPool* sessionPool)
    : _session(session), _sessionPool(sessionPool) {}

MobileSession::~MobileSession() {
    // Releases this session back to the session pool.
    _sessionPool->releaseSession(this);
}

sqlite3* MobileSession::getSession() const {
    return _session;
}
}  // namespace mongo
