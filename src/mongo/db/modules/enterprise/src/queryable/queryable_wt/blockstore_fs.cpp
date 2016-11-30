/**
*  Copyright (C) 2016 MongoDB Inc.
*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "blockstore_fs.h"

#include <string.h>

#include "mongo/base/string_data.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/allocator.h"
#include "mongo/util/log.h"

#include "../blockstore/http_client.h"

using mongo::operator""_sd;

extern "C" {

/*
 * Extension initialization function.
 */
static int queryableWtFsDirectoryList(WT_FILE_SYSTEM* file_system,
                                      WT_SESSION* session,
                                      const char* directory,
                                      const char* prefix,
                                      char*** dirlistp,
                                      uint32_t* countp) {
    // Actually not needed in readonly mode
    return 0;
}

static int queryableWtFsDirectoryListFree(WT_FILE_SYSTEM* file_system,
                                          WT_SESSION* session,
                                          char** dirlist,
                                          uint32_t count) {
    // Actually not needed in readonly mode
    return 0;
}

/*
 * Forward function declarations for file system API implementation
 */
static int queryableWtFsFileOpen(
    WT_FILE_SYSTEM*, WT_SESSION*, const char*, WT_FS_OPEN_FILE_TYPE, uint32_t, WT_FILE_HANDLE**);
static int queryableWtFsFileExist(WT_FILE_SYSTEM*, WT_SESSION*, const char*, bool*);
static int queryableWtFsFileSize(WT_FILE_SYSTEM*, WT_SESSION*, const char*, wt_off_t*);
static int queryableWtFsTerminate(WT_FILE_SYSTEM*, WT_SESSION*);

/*
 * Forward function declarations for file handle API implementation
 */
static int queryableWtFileRead(WT_FILE_HANDLE*, WT_SESSION*, wt_off_t, size_t, void*);
static int queryableWtFileSize(WT_FILE_HANDLE*, WT_SESSION*, wt_off_t*);
static int queryableWtFileLock(WT_FILE_HANDLE*, WT_SESSION*, bool);
static int queryableWtFileClose(WT_FILE_HANDLE*, WT_SESSION*);

/*
 * queryableWtFsCreate --
 *   Initialization point for the queryable file system
 */
int queryableWtFsCreate(WT_CONNECTION* conn, WT_CONFIG_ARG* config) {
    WT_CONFIG_ITEM k, v;
    WT_CONFIG_PARSER* config_parser;
    WT_EXTENSION_API* wtext;
    int ret = 0;

    std::string apiUri;
    std::string snapshotId;
    std::string dbpath;

    wtext = conn->get_extension_api(conn);

    // Open a WiredTiger parser on the "config" value.
    if ((ret = wtext->config_parser_open_arg(wtext, nullptr, config, &config_parser)) != 0) {
        (void)wtext->err_printf(wtext,
                                nullptr,
                                "WT_EXTENSION_API.config_parser_open: config: %s",
                                wtext->strerror(wtext, nullptr, ret));
        exit(1);
    }

    // Step through our configuration values.
    while ((ret = config_parser->next(config_parser, &k, &v)) == 0) {
        if (mongo::StringData(k.str, k.len) == "apiUri"_sd) {
            apiUri = std::string(v.str, v.len);
            continue;
        }
        if (mongo::StringData(k.str, k.len) == "snapshotId"_sd) {
            snapshotId = std::string(v.str, v.len);
            if (snapshotId.size() != 24) {
                (void)wtext->err_printf(wtext,
                                        nullptr,
                                        "snapshotId is not a valid OID. snapshotId: %.*s",
                                        (int)v.len,
                                        v.str);
                exit(1);
            }

            snapshotId = std::string(v.str, v.len);
            continue;
        }
        if (mongo::StringData(k.str, k.len) == "dbpath"_sd) {
            dbpath = std::string(v.str, v.len);
            continue;
        }

        (void)wtext->err_printf(wtext,
                                nullptr,
                                "WT_CONFIG_PARSER.next: unexpected configuration "
                                "information: %.*s=%.*s: %s",
                                (int)k.len,
                                k.str,
                                (int)v.len,
                                v.str,
                                wtext->strerror(wtext, nullptr, ret));
        exit(1);
    }

    // Check for expected parser termination and close the parser.
    if (ret != WT_NOTFOUND) {
        (void)wtext->err_printf(wtext,
                                nullptr,
                                "WT_CONFIG_PARSER.next: config: %s",
                                wtext->strerror(wtext, nullptr, ret));
        exit(1);
    }
    if ((ret = config_parser->close(config_parser)) != 0) {
        (void)wtext->err_printf(wtext,
                                nullptr,
                                "WT_CONFIG_PARSER.close: config: %s",
                                wtext->strerror(wtext, nullptr, ret));
        exit(1);
    }

    std::unique_ptr<mongo::queryable::HttpClientInterface> httpClient =
        mongo::queryable::createHttpClient(apiUri, mongo::OID(snapshotId));
    auto swFiles = listDirectory(httpClient.get());
    if (!swFiles.isOK()) {
        (void)wtext->err_printf(
            wtext, nullptr, "ListDir: %s", swFiles.getStatus().reason().c_str());
        exit(1);
    }

    auto blockstoreFs = mongo::stdx::make_unique<mongo::queryable::BlockstoreFileSystem>(
        std::move(apiUri), mongo::OID(snapshotId), std::move(dbpath), wtext);

    for (const auto& file : swFiles.getValue()) {
        blockstoreFs->addFile(file);
    }

    WT_FILE_SYSTEM* wtFileSystem = blockstoreFs->getWtFileSystem();
    memset(wtFileSystem, 0, sizeof(WT_FILE_SYSTEM));

    // Initialize the in-memory jump table.
    wtFileSystem->fs_open_file = queryableWtFsFileOpen;
    wtFileSystem->fs_exist = queryableWtFsFileExist;
    wtFileSystem->fs_size = queryableWtFsFileSize;
    wtFileSystem->terminate = queryableWtFsTerminate;

    wtFileSystem->fs_directory_list = queryableWtFsDirectoryList;
    wtFileSystem->fs_directory_list_free = queryableWtFsDirectoryListFree;

    if ((ret = conn->set_file_system(conn, wtFileSystem, nullptr)) != 0) {
        (void)wtext->err_printf(wtext,
                                nullptr,
                                "WT_CONNECTION.set_file_system: %s",
                                wtext->strerror(wtext, nullptr, ret));
        exit(1);
    }

    // WT will call a filesystem terminate method and pass in the pointer `blockstoreFs` for
    // cleaning up.
    blockstoreFs.release();
    return 0;
}

static int queryableWtFsFileOpen(WT_FILE_SYSTEM* file_system,
                                 WT_SESSION* session,
                                 const char* name,
                                 WT_FS_OPEN_FILE_TYPE file_type,
                                 uint32_t flags,
                                 WT_FILE_HANDLE** file_handlep) {
    auto blockstoreFs = reinterpret_cast<mongo::queryable::BlockstoreFileSystem*>(file_system);

    return blockstoreFs->open(
        name, reinterpret_cast<mongo::queryable::BlockstoreFileHandle**>(file_handlep));
}

static int queryableWtFsFileExist(WT_FILE_SYSTEM* file_system,
                                  WT_SESSION* session,
                                  const char* name,
                                  bool* existp) {
    auto blockstoreFs = reinterpret_cast<mongo::queryable::BlockstoreFileSystem*>(file_system);
    *existp = blockstoreFs->fileExists(name);

    return 0;
}

static int queryableWtFsFileSize(WT_FILE_SYSTEM* file_system,
                                 WT_SESSION* session,
                                 const char* name,
                                 wt_off_t* sizep) {
    auto blockstoreFs = reinterpret_cast<mongo::queryable::BlockstoreFileSystem*>(file_system);
    if (!blockstoreFs->fileExists(name)) {
        return ENOENT;
    }

    *sizep = blockstoreFs->getFileSize(name);
    return 0;
}

static int queryableWtFsTerminate(WT_FILE_SYSTEM* file_system, WT_SESSION* session) {
    auto blockstoreFs = reinterpret_cast<mongo::queryable::BlockstoreFileSystem*>(file_system);
    delete blockstoreFs;

    return 0;
}

static int queryableWtFileRead(
    WT_FILE_HANDLE* file_handle, WT_SESSION* session, wt_off_t offset, size_t len, void* buf) {
    auto fileHandle = reinterpret_cast<mongo::queryable::BlockstoreFileHandle*>(file_handle);

    return fileHandle->read(buf, offset, len);
}

static int queryableWtFileSize(WT_FILE_HANDLE* file_handle, WT_SESSION* session, wt_off_t* sizep) {
    auto fileHandle = reinterpret_cast<mongo::queryable::BlockstoreFileHandle*>(file_handle);

    *sizep = (wt_off_t)fileHandle->getFileSize();
    return 0;
}

static int queryableWtFileLock(WT_FILE_HANDLE* file_handle, WT_SESSION* session, bool lock) {
    // Locks are always granted.
    return 0;
}

static int queryableWtFileClose(WT_FILE_HANDLE* baseFileHandle, WT_SESSION* session) {
    free(baseFileHandle->name);

    auto blockstoreFileHandle =
        reinterpret_cast<mongo::queryable::BlockstoreFileHandle*>(baseFileHandle);
    delete blockstoreFileHandle;

    return 0;
}
}

namespace mongo {
namespace queryable {
namespace {
bool endsWith(const std::string& value, const std::string& ending) {
    if (ending.size() > value.size())
        return false;

    return value.compare(value.length() - ending.size(), ending.size(), ending) == 0;
}
}  // namespace

int BlockstoreFileSystem::open(const char* name, BlockstoreFileHandle** fileHandle) {
    std::string filename(name);
    if (endsWith(filename, "WiredTiger.lock")) {
        return ENOENT;
    }

    if (!fileExists(name)) {
        return ENOENT;
    }

    auto file = getFile(name);

    auto ret = stdx::make_unique<BlockstoreFileHandle>(
        this,
        stdx::make_unique<Reader>(
            createHttpClient(_apiUri, _snapshotId), file.filename, file.fileSize, file.blockSize),
        file.fileSize,
        file.blockSize);
    if (ret == nullptr) {
        return ENOMEM;
    }

    /* Initialize public information. */
    WT_FILE_HANDLE* baseFileHandle = ret->getWtFileHandle();
    memset(baseFileHandle, 0, sizeof(WT_FILE_HANDLE));
    if ((baseFileHandle->name = strdup(name)) == nullptr) {
        return ENOMEM;
    }

    /*
     * Setup the function call table for our custom file system. Set the function pointer to nullptr
     * where our implementation doesn't support the functionality.
     */
    baseFileHandle->close = queryableWtFileClose;
    baseFileHandle->fh_read = queryableWtFileRead;
    baseFileHandle->fh_size = queryableWtFileSize;
    baseFileHandle->fh_lock = queryableWtFileLock;

    *fileHandle = ret.release();
    return 0;
}

int BlockstoreFileHandle::read(void* buf, std::size_t offset, std::size_t length) {
    if (offset > static_cast<std::size_t>(_fileSize)) {
        return EINVAL;
    }

    if (length > _fileSize - offset) {
        length = _fileSize - offset;
    }

    int ret;
    std::string msg;
    mongo::DataRange wrappedBuf(reinterpret_cast<char*>(buf), length);
    auto swBytesRead = _reader->read(wrappedBuf, offset, length);
    if (!swBytesRead.isOK()) {
        ret = EIO;
        log() << swBytesRead.getStatus().reason();
    } else if (swBytesRead.getValue() != length) {
        ret = EAGAIN;
        log() << "Read < Length. Read: " << swBytesRead.getValue() << "Length: " << length;
    } else {
        ret = 0;
    }

    return ret;
}

}  // namespace queryable
}  // namespace mongo
