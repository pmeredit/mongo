/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mobile_kv_engine.h"

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/system/error_code.hpp>
#include <memory>
#include <vector>

#include "mobile_index.h"
#include "mobile_record_store.h"
#include "mobile_recovery_unit.h"
#include "mobile_session.h"
#include "mobile_sqlite_statement.h"
#include "mobile_util.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"

namespace mongo {

class MobileSession;
class SqliteStatement;

MobileKVEngine::MobileKVEngine(const std::string& path) {
    _initDBPath(path);

    // Initialize the database to be in WAL mode.
    sqlite3* initSession;
    int status = sqlite3_open(_path.c_str(), &initSession);
    checkStatus(status, SQLITE_OK, "sqlite3_open");

    // Guarantees that sqlite3_close() will be called when the function returns.
    ON_BLOCK_EXIT([&initSession] { sqlite3_close(initSession); });

    sqlite3_stmt* stmt;
    status = sqlite3_prepare_v2(initSession, "PRAGMA journal_mode=WAL;", -1, &stmt, NULL);
    checkStatus(status, SQLITE_OK, "sqlite3_prepare_v2");

    status = sqlite3_step(stmt);
    checkStatus(status, SQLITE_ROW, "sqlite3_step");

    // Pragma returns current mode in SQLite, ensure it is "wal" mode.
    const void* colText = sqlite3_column_text(stmt, 0);
    const char* mode = reinterpret_cast<const char*>(colText);
    fassert(37001, !strcmp(mode, "wal"));
    status = sqlite3_finalize(stmt);
    checkStatus(status, SQLITE_OK, "sqlite3_finalize");

    _sessionPool.reset(new MobileSessionPool(_path));
}

void MobileKVEngine::_initDBPath(const std::string& path) {
    boost::system::error_code err;
    boost::filesystem::path dbPath(path);

    if (!boost::filesystem::exists(dbPath, err)) {
        if (err) {
            uasserted(4085, err.message());
        }
        std::string errMsg("DB path not found: ");
        errMsg += dbPath.generic_string();
        uasserted(4086, errMsg);

    } else if (!boost::filesystem::is_directory(dbPath, err)) {
        if (err) {
            uasserted(4087, err.message());
        }
        std::string errMsg("DB path is not a valid directory: ");
        errMsg += dbPath.generic_string();
        uasserted(4088, errMsg);
    }

    dbPath /= "mobile.sqlite";

    if (boost::filesystem::exists(dbPath, err)) {
        if (err) {
            uasserted(4089, err.message());
        } else if (!boost::filesystem::is_regular_file(dbPath)) {
            std::string errMsg("Failed to open " + dbPath.generic_string() +
                               ": not a regular file");
            uasserted(4090, errMsg);
        }
    }
    _path = dbPath.generic_string();
}

RecoveryUnit* MobileKVEngine::newRecoveryUnit() {
    return new MobileRecoveryUnit(_sessionPool.get());
}

Status MobileKVEngine::createRecordStore(OperationContext* opCtx,
                                         StringData ns,
                                         StringData ident,
                                         const CollectionOptions& options) {
    // TODO: eventually will support file renaming but otherwise do not use collection options.
    MobileRecordStore::create(opCtx, ident.toString());
    return Status::OK();
}

std::unique_ptr<RecordStore> MobileKVEngine::getRecordStore(OperationContext* opCtx,
                                                            StringData ns,
                                                            StringData ident,
                                                            const CollectionOptions& options) {
    return stdx::make_unique<MobileRecordStore>(opCtx, ns, _path, ident.toString(), options);
}

Status MobileKVEngine::createSortedDataInterface(OperationContext* opCtx,
                                                 StringData ident,
                                                 const IndexDescriptor* desc) {
    return MobileIndex::create(opCtx, ident.toString());
}

SortedDataInterface* MobileKVEngine::getSortedDataInterface(OperationContext* opCtx,
                                                            StringData ident,
                                                            const IndexDescriptor* desc) {
    if (desc->unique()) {
        return new MobileIndexUnique(opCtx, desc, ident.toString());
    }
    return new MobileIndexStandard(opCtx, desc, ident.toString());
}

Status MobileKVEngine::dropIdent(OperationContext* opCtx, StringData ident) {
    MobileSession* session = MobileRecoveryUnit::get(opCtx)->getSessionNoTxn(opCtx);
    std::string dropQuery = "DROP TABLE IF EXISTS \"" + ident + "\";";

    try {
        SqliteStatement::execQuery(session, dropQuery.c_str());
    } catch (const WriteConflictException&) {
        // It is possible that this drop fails because of transaction running in parallel.
        // We pretend that it succeeded, queue it for now and keep retrying later.
        LOG(2) << "MobileSE: Caught WriteConflictException while dropping table, queuing to retry "
                  "later";
        MobileRecoveryUnit::get(opCtx)->enqueueFailedDrop(dropQuery);
    }
    return Status::OK();
}

/**
 * Note: this counts the total number of bytes in the key and value columns, not the actual number
 * of bytes on disk used by this ident.
 */
int64_t MobileKVEngine::getIdentSize(OperationContext* opCtx, StringData ident) {
    MobileSession* session = MobileRecoveryUnit::get(opCtx)->getSession(opCtx);

    // Get key-value column names.
    std::string colNameQuery = "PRAGMA table_info(\"" + ident + "\")";
    SqliteStatement colNameStmt(*session, colNameQuery);

    colNameStmt.step(SQLITE_ROW);
    std::string keyColName(static_cast<const char*>(colNameStmt.getColText(1)));
    colNameStmt.step(SQLITE_ROW);
    std::string valueColName(static_cast<const char*>(colNameStmt.getColText(1)));
    colNameStmt.step(SQLITE_DONE);

    // Get total data size of key-value columns.
    str::stream dataSizeQuery;
    dataSizeQuery << "SELECT IFNULL(SUM(LENGTH(" << keyColName << ")), 0) + "
                  << "IFNULL(SUM(LENGTH(" << valueColName << ")), 0) FROM \"" << ident + "\";";
    SqliteStatement dataSizeStmt(*session, dataSizeQuery);

    dataSizeStmt.step(SQLITE_ROW);
    return dataSizeStmt.getColInt(0);
}

bool MobileKVEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
    MobileSession* session = MobileRecoveryUnit::get(opCtx)->getSession(opCtx);

    std::string findTableQuery = "SELECT * FROM sqlite_master WHERE type='table' AND name = ?;";
    SqliteStatement findTableStmt(*session, findTableQuery);
    findTableStmt.bindText(0, ident.rawData(), ident.size());

    int status = findTableStmt.step();
    if (status == SQLITE_DONE) {
        return false;
    }
    checkStatus(status, SQLITE_ROW, "sqlite3_step");

    return true;
}

std::vector<std::string> MobileKVEngine::getAllIdents(OperationContext* opCtx) const {
    std::vector<std::string> idents;
    MobileSession* session = MobileRecoveryUnit::get(opCtx)->getSession(opCtx);
    std::string getTablesQuery = "SELECT name FROM sqlite_master WHERE type='table';";
    SqliteStatement getTablesStmt(*session, getTablesQuery);

    int status;
    while ((status = getTablesStmt.step()) == SQLITE_ROW) {
        std::string tableName(reinterpret_cast<const char*>(getTablesStmt.getColText(0)));
        idents.push_back(tableName);
    }
    checkStatus(status, SQLITE_DONE, "sqlite3_step");
    return idents;
}

}  // namespace mongo
