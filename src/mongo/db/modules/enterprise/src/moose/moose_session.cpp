/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "moose_session.h"

#include "../third_party/sqlite/sqlite3.h"
#include "moose_session_pool.h"

namespace mongo {

MooseSession::MooseSession(sqlite3* session, MooseSessionPool* sessionPool)
    : _session(session), _sessionPool(sessionPool) {}

MooseSession::~MooseSession() {
    // Releases this session back to the session pool.
    _sessionPool->releaseSession(this);
}

sqlite3* MooseSession::getSession() const {
    return _session;
}
}  // namespace mongo
