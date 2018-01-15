/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#include "../third_party/sqlite/sqlite3.h"
#include "mobile_recovery_unit.h"
#include "mobile_sqlite_statement.h"
#include "mobile_util.h"

namespace mongo {

using std::string;

Status sqliteRCToStatus(int retCode, const char* prefix) {
    str::stream s;
    if (prefix)
        s << prefix << " ";
    s << retCode << ": " << sqlite3_errstr(retCode);

    switch (retCode) {
        case SQLITE_OK:
            return Status::OK();
        case SQLITE_INTERNAL:
            return Status(ErrorCodes::InternalError, s);
        case SQLITE_PERM:
            return Status(ErrorCodes::Unauthorized, s);
        case SQLITE_BUSY:
            return Status(ErrorCodes::LockBusy, s);
        case SQLITE_LOCKED:
            return Status(ErrorCodes::LockBusy, s);
        case SQLITE_NOMEM:
            return Status(ErrorCodes::ExceededMemoryLimit, s);
        case SQLITE_READONLY:
            return Status(ErrorCodes::Unauthorized, s);
        case SQLITE_INTERRUPT:
            return Status(ErrorCodes::Interrupted, s);
        case SQLITE_CANTOPEN:
            return Status(ErrorCodes::FileOpenFailed, s);
        case SQLITE_PROTOCOL:
            return Status(ErrorCodes::ProtocolError, s);
        case SQLITE_MISMATCH:
            return Status(ErrorCodes::TypeMismatch, s);
        case SQLITE_MISUSE:
            return Status(ErrorCodes::BadValue, s);
        case SQLITE_NOLFS:
            return Status(ErrorCodes::CommandNotSupported, s);
        case SQLITE_AUTH:
            return Status(ErrorCodes::AuthenticationFailed, s);
        case SQLITE_FORMAT:
            return Status(ErrorCodes::UnsupportedFormat, s);
        case SQLITE_RANGE:
            return Status(ErrorCodes::BadValue, s);
        case SQLITE_NOTADB:
            return Status(ErrorCodes::FileOpenFailed, s);
        default:
            return Status(ErrorCodes::UnknownError, s);
    }
}

void checkStatus(int retStatus, int desiredStatus, const char* fnName, const char* errMsg) {
    if (retStatus != desiredStatus) {
        std::stringstream s;
        s << fnName << " failed with return status " << sqlite3_errstr(retStatus);

        if (errMsg) {
            s << "------ Error Message: " << errMsg;
        }

        severe() << s.str();
        fassertFailed(37000);
    }
}

/**
 * Helper to add and log errors for validate.
 */
void validateLogAndAppendError(ValidateResults* results, const std::string& errMsg) {
    error() << "validate found error: " << errMsg;
    results->errors.push_back(errMsg);
    results->valid = false;
}

void doValidate(OperationContext* opCtx, ValidateResults* results) {
    MobileSession* session = MobileRecoveryUnit::get(opCtx)->getSession(opCtx);
    std::string validateQuery = "PRAGMA integrity_check;";
    try {
        SqliteStatement validateStmt(*session, validateQuery);

        int status;
        // By default, the integrity check returns the first 100 errors found.
        while ((status = validateStmt.step()) == SQLITE_ROW) {
            std::string errMsg(reinterpret_cast<const char*>(validateStmt.getColText(0)));

            if (errMsg == "ok") {
                // If the first message returned is "ok", the integrity check passed without
                // finding any corruption.
                continue;
            }

            validateLogAndAppendError(results, errMsg);
        }

        if (status == SQLITE_CORRUPT) {
            uasserted(ErrorCodes::UnknownError, sqlite3_errstr(status));
        }
        checkStatus(status, SQLITE_DONE, "sqlite3_step");

    } catch (const DBException& e) {
        // SQLite statement may fail to prepare or execute correctly if the file is corrupted.
        std::string errMsg = "database file is corrupt - " + e.toString();
        validateLogAndAppendError(results, errMsg);
    }
}

}  // namespace mongo
