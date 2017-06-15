/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "moose_sqlite_statement.h"

#include <string>

#include "../third_party/sqlite/sqlite3.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/scopeguard.h"
#include "moose_util.h"

namespace mongo {

SqliteStatement::SqliteStatement(const MooseSession& session, const std::string& sqlQuery) {
    int status = sqlite3_prepare_v2(
        session.getSession(), sqlQuery.c_str(), sqlQuery.length() + 1, &_stmt, NULL);
    if (status == SQLITE_BUSY) {
        throw new WriteConflictException();
    } else if (status != SQLITE_OK) {
        std::string errMsg = "sqlite3_prepare_v2 failed: ";
        errMsg += sqlite3_errstr(status);
        throw new DBException(ErrorCodes::UnknownError, errMsg);
    }
}

SqliteStatement::~SqliteStatement() {
    int status = sqlite3_finalize(_stmt);
    fassert(37053, status == _exceptionStatus);
}

void SqliteStatement::bindInt(int paramIndex, int64_t intValue) {
    // SQLite bind methods begin paramater indexes at 1 rather than 0.
    int status = sqlite3_bind_int64(_stmt, paramIndex + 1, intValue);
    checkStatus(status, SQLITE_OK, "sqlite3_bind");
}

void SqliteStatement::bindBlob(int paramIndex, const void* data, int len) {
    // SQLite bind methods begin paramater indexes at 1 rather than 0.
    int status = sqlite3_bind_blob(_stmt, paramIndex + 1, data, len, SQLITE_STATIC);
    checkStatus(status, SQLITE_OK, "sqlite3_bind");
}

void SqliteStatement::bindText(int paramIndex, const char* data, int len) {
    // SQLite bind methods begin paramater indexes at 1 rather than 0.
    int status = sqlite3_bind_text(_stmt, paramIndex + 1, data, len, SQLITE_STATIC);
    checkStatus(status, SQLITE_OK, "sqlite3_bind");
}

void SqliteStatement::clearBindings() {
    int status = sqlite3_clear_bindings(_stmt);
    checkStatus(status, SQLITE_OK, "sqlite3_clear_bindings");
}

int SqliteStatement::step(int desiredStatus) {
    int status = sqlite3_step(_stmt);

    // A non-negative desiredStatus indicates that checkStatus should assert that the returned
    // status is equivalent to the desired status.
    if (desiredStatus >= 0) {
        checkStatus(status, desiredStatus, "sqlite3_step");
    }

    return status;
}

int64_t SqliteStatement::getColInt(int colIndex) {
    return sqlite3_column_int64(_stmt, colIndex);
}

const void* SqliteStatement::getColBlob(int colIndex) {
    return sqlite3_column_blob(_stmt, colIndex);
}

int64_t SqliteStatement::getColBytes(int colIndex) {
    return sqlite3_column_bytes(_stmt, colIndex);
}

const void* SqliteStatement::getColText(int colIndex) {
    return sqlite3_column_text(_stmt, colIndex);
}

void SqliteStatement::execQuery(MooseSession* session, const std::string& query) {
    char* errMsg = NULL;
    int status = sqlite3_exec(session->getSession(), query.c_str(), NULL, NULL, &errMsg);

    if (status == SQLITE_BUSY || status == SQLITE_LOCKED) {
        throw WriteConflictException();
    }

    // The only return value from sqlite3_exec in a success case is SQLITE_OK.
    checkStatus(status, SQLITE_OK, "sqlite3_exec", errMsg);

    // When the error message is not NULL, it is allocated through sqlite3_malloc and must be freed
    // before exiting the method. If the error message is NULL, sqlite3_free is a no-op.
    sqlite3_free(errMsg);
}

void SqliteStatement::reset() {
    int status = sqlite3_reset(_stmt);
    checkStatus(status, SQLITE_OK, "sqlite3_reset");
}

}  // namespace mongo
