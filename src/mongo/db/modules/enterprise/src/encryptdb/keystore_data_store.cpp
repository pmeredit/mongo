/**
 * Copyright (c) 2019 MongoDB, Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "keystore_data_store.h"

#include "boost/filesystem.hpp"

#include "encryption_key_manager.h"
#include "mongo/base/data_builder.h"
#include "mongo/base/status.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_util.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace fs = boost::filesystem;
namespace {
int keystore_handle_error(WT_EVENT_HANDLER* handler,
                          WT_SESSION* session,
                          int errorCode,
                          const char* message) {
    try {
        error() << "WiredTiger keystore (" << errorCode << ") " << message;
        fassert(4051, errorCode != WT_PANIC);
    } catch (...) {
        std::terminate();
    }
    return 0;
}

int keystore_handle_message(WT_EVENT_HANDLER* handler, WT_SESSION* session, const char* message) {
    try {
        log() << "WiredTiger keystore " << message;
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
        log() << "WiredTiger keystore progress " << operation << " " << progress;
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

void uassertWTOK(int ret) {
    return uassertStatusOK(wtRCToStatus(ret));
}

bool WTDataStoreCursor::advance() {
    invariant(_cursor);
    auto ret = _cursor->next(_cursor.get());
    if (ret == WT_NOTFOUND) {
        return false;
    } else {
        uassertStatusOK(wtRCToStatus(ret));
    }

    return true;
}

WTDataStoreCursor& WTDataStoreCursor::operator++() {
    if (!_cursor) {
        return *this;
    }
    if (!advance()) {
        _cursor.reset();
    }
    return *this;
}

UniqueWTCursor WTDataStoreCursor::_duplicate(const UniqueWTCursor& other) {
    WT_CURSOR* ptr = nullptr;
    WT_SESSION* session = other->session;
    auto rc = session->open_cursor(session, nullptr, other.get(), nullptr, &ptr);
    UniqueWTCursor cursor(ptr);
    uassertWTOK(rc);

    return cursor;
}

void WTDataStoreCursor::release() {
    _cursor.release();
}

void WTDataStoreCursor::close() {
    _cursor.reset();
}

UniqueWTCursor WTDataStoreSession::_makeCursor(StringData uri) {
    WT_CURSOR* cursorPtr = nullptr;
    auto rc = _session->open_cursor(_session.get(), uri.rawData(), nullptr, nullptr, &cursorPtr);
    UniqueWTCursor cursor(cursorPtr);
    if (rc == WT_NOTFOUND || rc == ENOENT) {
        return nullptr;
    }
    uassertWTOK(rc);
    return cursor;
}

WTDataStoreSession::iterator WTDataStoreSession::begin() {
    iterator it(_makeCursor(kKeystoreTableName));
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
    wtConfig << "log=(enabled,file_max=3MB),transaction_sync=(enabled=true,method=fsync),";
    wtConfig << "extensions=[" << kEncryptionEntrypointConfig << "],";
    wtConfig << _keystoreConfig;
    if (encryptionParams->readOnlyMode) {
        wtConfig << "readonly=true,";
    }

    log() << "Opening WiredTiger keystore. Config: " << wtConfig.str();

    WT_CONNECTION* connPtr = nullptr;
    auto rc = wiredtiger_open(
        path.string().c_str(), keystoreEventHandlers(), wtConfig.str().c_str(), &connPtr);
    _connection.reset(connPtr);

    uassertWTOK(rc);
}

void WTDataStore::close() {
    _connection.reset();
}

WTDataStoreSession WTDataStore::makeSession() {
    WT_SESSION* sessionPtr = nullptr;
    uassertWTOK(_connection->open_session(_connection.get(), nullptr, nullptr, &sessionPtr));
    return UniqueWTSession(sessionPtr);
}

void WTDataStore::createTable(const WTDataStoreSession& session, StringData config) {
    std::string fullConfig = str::stream() << _keystoreConfig << config;
    uassertWTOK(session->create(session, kKeystoreTableName.rawData(), fullConfig.c_str()));
}

}  // namespace mongo
