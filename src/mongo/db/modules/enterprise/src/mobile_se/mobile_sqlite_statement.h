/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#pragma once

#include <string>

#include "../third_party/sqlite/sqlite3.h"

#include "mobile_session.h"

namespace mongo {

/**
 * SqliteStatement is a wrapper around the sqlite3_stmt object. All calls to the SQLite API that
 * involve a sqlite_stmt object are made in this class.
 */
class SqliteStatement final {
public:
    /**
     * Creates and prepares a SQLite statement.
     */
    SqliteStatement(const MobileSession& session, const std::string& sqlQuery);

    /**
     * Finalizes the prepared statement.
     */
    ~SqliteStatement();

    /**
     * The various bind methods bind a value to the query parameter specified by paramIndex.
     *
     * @param paramIndex - zero-based index of a query parameter.
     */
    void bindInt(int paramIndex, int64_t intValue);

    void bindBlob(int paramIndex, const void* data, int len);

    void bindText(int paramIndex, const char* data, int len);

    void clearBindings();

    /**
     * Wraps sqlite3_step and returns the resulting status.
     *
     * @param desiredStatus - the desired return status of sqlite3_step. When desiredStatus is
     * non-negative, checkStatus compares desiredStatus with the returned status from sqlite3_step.
     * By default, checkStatus is ignored.
     */
    int step(int desiredStatus = -1);

    /**
     * The getCol methods wrap sqlite3_column methods and return the correctly typed values
     * stored in a retrieved query row[colIndex].
     *
     * @param colIndex - zero-based index of a column retrieved from a query row.
     */
    int64_t getColInt(int colIndex);

    const void* getColBlob(int colIndex);

    /**
     * Returns the number of bytes in a corresponding blob or string.
     */
    int64_t getColBytes(int colIndex);

    /**
     * Wraps sqlite3_column_text method and returns the text from the retrieved query row[colIndex].
     *
     * @param colIndex - zero-based index of a column retrieved from a query row.
     */
    const void* getColText(int colIndex);

    /**
     * Resets the statement to the first of the query result rows.
     */
    void reset();

    /**
     * Sets the last status on the prepared statement.
     */
    void setExceptionStatus(int status) {
        _exceptionStatus = status;
    }

    /**
     * A one step query execution that wraps sqlite3_prepare_v2(), sqlite3_step(), and
     * sqlite3_finalize().
     * None of the rows retrieved, if any, are saved before the query is finalized. Thus, this
     * method should not be used for read operations.
     */
    static void execQuery(MobileSession* session, const std::string& query);

private:
    sqlite3_stmt* _stmt;

    // If the most recent call to sqlite3_step on this statement returned an error, the error is
    // returned again when the statement is finalized. This is used to verify that the last error
    // code returned matches the finalize error code, if there is any.
    int _exceptionStatus = SQLITE_OK;
};
}  // namespace mongo
