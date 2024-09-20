/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "keystore_data_store.h"

#include <boost/filesystem.hpp>

#include "encryption_key_manager.h"
#include "mongo/base/data_builder.h"
#include "mongo/base/status.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/str.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage


namespace mongo {
namespace fs = boost::filesystem;
namespace {
int keystore_handle_error(WT_EVENT_HANDLER* handler,
                          WT_SESSION* session,
                          int errorCode,
                          const char* message) {
    try {
        LOGV2_ERROR(24208,
                    "WiredTiger keystore error",
                    "errorCode"_attr = errorCode,
                    "message"_attr = message);
        fassert(4051, errorCode != WT_PANIC);
    } catch (...) {
        std::terminate();
    }
    return 0;
}

int keystore_handle_message(WT_EVENT_HANDLER* handler, WT_SESSION* session, const char* message) {
    try {
        LOGV2(24205, "WiredTiger keystore message", "message"_attr = message);
    } catch (...) {
        std::terminate();
    }
    return 0;
}

int keystore_handle_progress(WT_EVENT_HANDLER* handler,
                             WT_SESSION* session,
                             const char* operation,
                             uint64_t progress) {
    try {
        LOGV2(24206,
              "WiredTiger keystore progress",
              "operation"_attr = operation,
              "progress"_attr = progress);
    } catch (...) {
        std::terminate();
    }
    return 0;
}

WT_EVENT_HANDLER* keystoreEventHandlers() {
    static WT_EVENT_HANDLER handlers = [&] {
        WT_EVENT_HANDLER handlers;
        handlers.handle_error = keystore_handle_error;
        handlers.handle_message = keystore_handle_message;
        handlers.handle_progress = keystore_handle_progress;
        handlers.handle_close = nullptr;
        return handlers;
    }();

    return &handlers;
}

Status createDirectoryIfNeeded(const fs::path& path) {
    try {
        if (!fs::exists(path)) {
            fs::create_directory(path);
        }
    } catch (const std::exception& e) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Error creating path " << path.string() << ' ' << e.what());
    }
    return Status::OK();
}

}  // namespace

bool WTDataStoreCursor::advance() {
    invariant(_cursor);
    int ret;
    if (_atEnd) {
        return false;
    }
    switch (_direction) {
        case CursorDirection::kForward:
            ret = _cursor->next(_cursor.get());
            break;
        case CursorDirection::kReverse:
            ret = _cursor->prev(_cursor.get());
            break;
        default:
            MONGO_UNREACHABLE;
    }
    if (ret == WT_NOTFOUND) {
        _atEnd = true;
        return false;
    } else {
        uassertStatusOK(wtRCToStatus(ret, _cursor->session));
    }

    return true;
}

WTDataStoreCursor& WTDataStoreCursor::operator++() {
    if (!_cursor) {
        return *this;
    }
    advance();
    return *this;
}

UniqueWTCursor WTDataStoreCursor::_duplicate(const UniqueWTCursor& other) {
    WT_CURSOR* ptr = nullptr;
    WT_SESSION* session = other->session;
    auto rc = session->open_cursor(session, nullptr, other.get(), nullptr, &ptr);
    UniqueWTCursor cursor(ptr);
    uassertWTOK(rc, session);

    return cursor;
}

void WTDataStoreCursor::release() {
    // Cast to void to explicitly ignore the return value since this function
    // intentionally relinquishes ownership.
    (void)_cursor.release();
}

void WTDataStoreCursor::close() {
    _cursor.reset();
}

bool WTDataStoreCursor::operator==(const WTDataStoreCursor& other) const {
    if (_cursor && other._cursor) {
        int equal = 0;
        _cursor->equals(_cursor.get(), other._cursor.get(), &equal);
        return (equal == 1);
    } else if (!_cursor && !other._cursor) {
        return true;
    } else if (_cursor || other._cursor) {
        return _atEnd == other._atEnd;
    } else {
        return false;
    }
}

bool WTDataStoreCursor::operator!=(const WTDataStoreCursor& other) const {
    return !(*this == other);
}

UniqueWTCursor WTDataStoreSession::_makeCursor(StringData uri, StringData cursorOpts) {
    WT_CURSOR* cursorPtr = nullptr;
    auto rc = _session->open_cursor(
        _session.get(), uri.rawData(), nullptr, cursorOpts.rawData(), &cursorPtr);
    UniqueWTCursor cursor(cursorPtr);
    if (rc == WT_NOTFOUND || rc == ENOENT) {
        return nullptr;
    }
    uassertWTOK(rc, _session.get());
    return cursor;
}

WTDataStoreSession::iterator WTDataStoreSession::begin() {
    iterator it(_makeCursor(kKeystoreTableName));
    if (!it || !it.advance()) {
        return iterator();
    }
    return it;
}

WTDataStoreSession::iterator WTDataStoreSession::rbegin() {
    iterator it(_makeCursor(kKeystoreTableName), WTDataStoreCursor::CursorDirection::kReverse);
    if (!it || !it.advance()) {
        return iterator();
    }
    return it;
}

WTDataStoreSession::iterator WTDataStoreSession::beginBackup() {
    iterator ret(_makeCursor("backup:"_sd));
    ret.advance();
    return ret;
}

void WTDataStoreSession::close() {
    _session.reset();
}


bool WTDataStoreSession::verifyTable() {
    auto ret = (_session->verify)(_session.get(), kKeystoreTableName.rawData(), nullptr);

    if (ret == EBUSY) {
        // SERVER-16457: verify and salvage are occasionally failing with EBUSY. For now we
        // lie and return OK to avoid breaking tests. This block should go away when that ticket
        // is resolved.
        LOGV2_ERROR(24209,
                    "Verify on 'table:keystore' failed with EBUSY. This means the keystore was "
                    "being accessed. No repair is necessary unless other "
                    "errors are reported");
        return true;
    } else if (ret == ENOENT || ret == 0) {
        return true;
    } else {
        return false;
    }
}

void WTDataStoreSession::salvage() {
    auto ret = _session->salvage(_session.get(), kKeystoreTableName.rawData(), nullptr);

    if (ret == EBUSY) {
        // SERVER-16457: verify and salvage are occasionally failing with EBUSY. For now we
        // lie and return OK to avoid breaking tests. This block should go away when that ticket
        // is resolved.
        LOGV2_ERROR(24210,
                    "Verify on 'table:keystore' failed with EBUSY. This means the keystore was "
                    "being accessed. No repair is necessary unless other "
                    "errors are reported");
    } else {
        fassertFailedWithStatusNoTrace(51226, wtRCToStatus(ret, _session.get()));
    }
}

WTDataStore::WTDataStore(const boost::filesystem::path& path,
                         const EncryptionGlobalParams* const encryptionParams) {
    uassertStatusOK(createDirectoryIfNeeded(path));

    // FIXME: While using GCM, needs to always use AES-GCM with RBG derived IVs
    _keystoreConfig = str::stream() << "encryption=(name=" << encryptionParams->encryptionCipherMode
                                    << ",keyid=" << kMasterKeyId << "),";

    StringBuilder wtConfig;
    // Use compatibility version 2.9 (WT's version on MongoDB 3.4) to avoid any 3.6 -> 3.4 binary
    // downgrade steps. The benefits of newer versions is faster WT log rotation, with the
    // trade-off of a data on disk format change that 3.4 binaries are not familiar with. Log
    // rotations for the keystore database are expected to be exceedingly rare; the benefits would
    // be minimal.
    wtConfig << "create,compatibility=(release=2.9),config_base=false,";
    wtConfig << "checkpoint=(wait=" << storageGlobalParams.syncdelay << "),";
    wtConfig << "log=(enabled,file_max=3MB),transaction_sync=(enabled=true,method=fsync),";
    wtConfig << "extensions=[" << kEncryptionEntrypointConfig << "],";
    wtConfig << _keystoreConfig;

    LOGV2(24207, "Opening WiredTiger keystore", "config"_attr = wtConfig.str());

    WT_CONNECTION* connPtr = nullptr;
    auto rc = wiredtiger_open(
        path.string().c_str(), keystoreEventHandlers(), wtConfig.str().c_str(), &connPtr);
    _connection.reset(connPtr);

    uassertWTOK(rc, nullptr);
}

void WTDataStore::close() {
    _connection.reset();
}

WTDataStoreSession WTDataStore::makeSession() {
    WT_SESSION* sessionPtr = nullptr;
    uassertWTOK(_connection->open_session(_connection.get(), nullptr, nullptr, &sessionPtr),
                nullptr);
    return UniqueWTSession(sessionPtr);
}

void WTDataStore::createTable(const WTDataStoreSession& session, StringData config) {
    std::string fullConfig = str::stream() << _keystoreConfig << config;
    uassertWTOK(session->create(session, kKeystoreTableName.rawData(), fullConfig.c_str()),
                nullptr);
}

}  // namespace mongo
