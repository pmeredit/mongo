/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "moose_kv_engine.h"

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/system/error_code.hpp>
#include <memory>
#include <vector>

#include "mongo/db/index/index_descriptor.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/scopeguard.h"
#include "moose_index.h"
#include "moose_record_store.h"
#include "moose_recovery_unit.h"
#include "moose_session.h"
#include "moose_sqlite_statement.h"
#include "moose_util.h"

namespace mongo {

class MooseSession;
class SqliteStatement;

MooseKVEngine::MooseKVEngine(const std::string& path) {
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

    _sessionPool.reset(new MooseSessionPool(_path));
}

void MooseKVEngine::_initDBPath(const std::string& path) {
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

    dbPath /= "moose.sqlite";

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

RecoveryUnit* MooseKVEngine::newRecoveryUnit() {
    return new MooseRecoveryUnit(_sessionPool.get());
}

Status MooseKVEngine::createRecordStore(OperationContext* opCtx,
                                        StringData ns,
                                        StringData ident,
                                        const CollectionOptions& options) {
    // TODO: eventually will support file renaming but otherwise do not use collection options.
    MooseRecordStore::create(opCtx, ident.toString());
    return Status::OK();
}

std::unique_ptr<RecordStore> MooseKVEngine::getRecordStore(OperationContext* opCtx,
                                                           StringData ns,
                                                           StringData ident,
                                                           const CollectionOptions& options) {
    return stdx::make_unique<MooseRecordStore>(opCtx, ns, _path, ident.toString(), options);
}

Status MooseKVEngine::createSortedDataInterface(OperationContext* opCtx,
                                                StringData ident,
                                                const IndexDescriptor* desc) {
    return MooseIndex::create(opCtx, ident.toString());
}

SortedDataInterface* MooseKVEngine::getSortedDataInterface(OperationContext* opCtx,
                                                           StringData ident,
                                                           const IndexDescriptor* desc) {
    if (desc->unique()) {
        return new MooseIndexUnique(opCtx, desc, ident.toString());
    }
    return new MooseIndexStandard(opCtx, desc, ident.toString());
}

Status MooseKVEngine::dropIdent(OperationContext* opCtx, StringData ident) {
    MooseSession* session = MooseRecoveryUnit::get(opCtx)->getSessionNoTxn(opCtx);
    std::string dropQuery = "DROP TABLE IF EXISTS \"" + ident + "\";";
    SqliteStatement::execQuery(session, dropQuery.c_str());
    return Status::OK();
}

/**
 * Note: this counts the total number of bytes in the key and value columns, not the actual number
 * of bytes on disk used by this ident.
 */
int64_t MooseKVEngine::getIdentSize(OperationContext* opCtx, StringData ident) {
    MooseSession* session = MooseRecoveryUnit::get(opCtx)->getSession(opCtx);

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

bool MooseKVEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
    MooseSession* session = MooseRecoveryUnit::get(opCtx)->getSession(opCtx);

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

std::vector<std::string> MooseKVEngine::getAllIdents(OperationContext* opCtx) const {
    std::vector<std::string> idents;
    MooseSession* session = MooseRecoveryUnit::get(opCtx)->getSession(opCtx);
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
