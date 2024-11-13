/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "blockstore_fs.h"

#include <cstring>
#include <memory>

#include "mongo/base/string_data.h"
#include "mongo/logv2/log.h"
#include "mongo/util/allocator.h"
#include "mongo/util/exit_code.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage


using mongo::operator""_sd;

extern "C" {

/*
 * Extension initialization function.
 */
int queryableWtFsDirectoryList(WT_FILE_SYSTEM* file_system,
                               WT_SESSION* session,
                               const char* directory,
                               const char* prefix,
                               char*** dirlistp,
                               uint32_t* countp) {
    auto blockstoreFs = reinterpret_cast<mongo::queryable::BlockstoreFileSystem*>(file_system);
    std::vector<std::string> files = blockstoreFs->getFiles(directory, prefix);
    if (files.empty()) {
        *countp = 0;
        return 0;
    }

    *dirlistp = static_cast<char**>(malloc(sizeof(char*) * files.size()));
    if (*dirlistp == NULL) {
        return ENOMEM;
    }

    for (size_t i = 0; i < files.size(); i++) {
        const std::string fileName = files[i];
        (*dirlistp)[i] = strdup(fileName.c_str());
        if ((*dirlistp)[i] == NULL) {
            return ENOMEM;
        }
    }

    *countp = files.size();
    return 0;
}

int queryableWtFsDirectoryListFree(WT_FILE_SYSTEM* file_system,
                                   WT_SESSION* session,
                                   char** dirlist,
                                   uint32_t count) {
    for (size_t i = 0; i < count; i++) {
        free(dirlist[i]);
    }
    free(dirlist);
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
static int queryableWtFsRemove(WT_FILE_SYSTEM*, WT_SESSION*, const char*, uint32_t);
static int queryableWtFsRename(WT_FILE_SYSTEM*, WT_SESSION*, const char*, const char*, uint32_t);

/*
 * Forward function declarations for file handle API implementation
 */
static int queryableWtFileRead(WT_FILE_HANDLE*, WT_SESSION*, wt_off_t, size_t, void*);
static int queryableWtFileSize(WT_FILE_HANDLE*, WT_SESSION*, wt_off_t*);
static int queryableWtFileLock(WT_FILE_HANDLE*, WT_SESSION*, bool);
static int queryableWtFileClose(WT_FILE_HANDLE*, WT_SESSION*);
static int queryableWtFileSync(WT_FILE_HANDLE*, WT_SESSION*);

/*
 * queryableWtFsCreate --
 *   Initialization point for the queryable file system
 */
MONGO_COMPILER_API_EXPORT int queryableWtFsCreate(WT_CONNECTION* conn, WT_CONFIG_ARG* config);
MONGO_COMPILER_API_EXPORT int queryableWtFsCreate(WT_CONNECTION* conn, WT_CONFIG_ARG* config) {
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
        exit(static_cast<int>(mongo::ExitCode::fail));
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
                exit(static_cast<int>(mongo::ExitCode::fail));
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
        exit(static_cast<int>(mongo::ExitCode::fail));
    }

    // Check for expected parser termination and close the parser.
    if (ret != WT_NOTFOUND) {
        (void)wtext->err_printf(wtext,
                                nullptr,
                                "WT_CONFIG_PARSER.next: config: %s",
                                wtext->strerror(wtext, nullptr, ret));
        exit(static_cast<int>(mongo::ExitCode::fail));
    }
    if ((ret = config_parser->close(config_parser)) != 0) {
        (void)wtext->err_printf(wtext,
                                nullptr,
                                "WT_CONFIG_PARSER.close: config: %s",
                                wtext->strerror(wtext, nullptr, ret));
        exit(static_cast<int>(mongo::ExitCode::fail));
    }

    auto swFiles = listDirectory(mongo::queryable::BlockstoreHTTP(apiUri, mongo::OID(snapshotId)));
    if (!swFiles.isOK()) {
        (void)wtext->err_printf(
            wtext, nullptr, "ListDir: %s", swFiles.getStatus().reason().c_str());
        exit(static_cast<int>(mongo::ExitCode::fail));
    }

    auto blockstoreFs = std::make_unique<mongo::queryable::BlockstoreFileSystem>(
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
    wtFileSystem->fs_remove = queryableWtFsRemove;
    wtFileSystem->fs_rename = queryableWtFsRename;

    if ((ret = conn->set_file_system(conn, wtFileSystem, nullptr)) != 0) {
        (void)wtext->err_printf(wtext,
                                nullptr,
                                "WT_CONNECTION.set_file_system: %s",
                                wtext->strerror(wtext, nullptr, ret));
        exit(static_cast<int>(mongo::ExitCode::fail));
    }

    // WT will call a filesystem terminate method and pass in the pointer `blockstoreFs` for
    // cleaning up. Cast to void to explicitly ignore the return value.
    (void)blockstoreFs.release();
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
        name, flags, reinterpret_cast<mongo::queryable::BlockstoreFileHandle**>(file_handlep));
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

static int queryableWtFsRemove(WT_FILE_SYSTEM* file_system,
                               WT_SESSION* session,
                               const char* name,
                               uint32_t flags) {
    // WiredTiger creates and removes metadata files during its initialization, but we don't really
    // need to remove these files.
    return 0;
}

static int queryableWtFsRename(WT_FILE_SYSTEM* file_system,
                               WT_SESSION* session,
                               const char* from,
                               const char* to,
                               uint32_t flags) {
    auto blockstoreFs = reinterpret_cast<mongo::queryable::BlockstoreFileSystem*>(file_system);
    return blockstoreFs->rename(from, to);
}

static int queryableWtFileRead(
    WT_FILE_HANDLE* file_handle, WT_SESSION* session, wt_off_t offset, size_t len, void* buf) {
    auto fileHandle = reinterpret_cast<mongo::queryable::BlockstoreFileHandle*>(file_handle);

    return fileHandle->read(buf, offset, len);
}

static int queryableWtFileWrite(WT_FILE_HANDLE* file_handle,
                                WT_SESSION* session,
                                wt_off_t offset,
                                size_t len,
                                const void* buf) {
    auto fileHandle = reinterpret_cast<mongo::queryable::BlockstoreFileHandle*>(file_handle);

    return fileHandle->write(buf, offset, len);
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

static int queryableWtFileSync(WT_FILE_HANDLE* baseFileHandle, WT_SESSION* session) {
    // Files are considered durable when the write HTTP POST request returns successfully.
    return 0;
}
}

namespace mongo {
namespace queryable {
int BlockstoreFileSystem::open(const char* name,
                               uint32_t flags,
                               BlockstoreFileHandle** fileHandle) {
    std::string filename(name);

    BlockstoreHTTP blockstoreHTTP(_apiUri, _snapshotId);

    // Send an API request to create the file if it doesn't already exist and add it to the list of
    // existing files on success.
    if (!fileExists(name)) {
        StatusWith<DataBuilder> swOpenFileResponse =
            blockstoreHTTP.openFile(getFileRelativePath(filename));
        if (!swOpenFileResponse.isOK()) {
            LOGV2_ERROR(
                24220,
                "Bad HTTP response from the ApiServer: {swOpenFileResponse_getStatus_reason}",
                "swOpenFileResponse_getStatus_reason"_attr =
                    swOpenFileResponse.getStatus().reason());
            return ENOENT;
        }

        struct File file;
        file.filename = getFileRelativePath(filename);
        file.fileSize = 0;

        addFile(std::move(file));
    }

    auto file = getFile(name);

    auto ret = std::make_unique<BlockstoreFileHandle>(
        this,
        std::make_unique<ReaderWriter>(std::move(blockstoreHTTP), file.filename, file.fileSize),
        file.fileSize);
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
    baseFileHandle->fh_write = queryableWtFileWrite;
    baseFileHandle->fh_size = queryableWtFileSize;
    baseFileHandle->fh_lock = queryableWtFileLock;
    baseFileHandle->fh_sync = queryableWtFileSync;

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

    mongo::DataRange wrappedBuf(reinterpret_cast<char*>(buf), length);
    auto swBytesRead = _readerWriter->read(wrappedBuf, offset, length);
    if (!swBytesRead.isOK()) {
        LOGV2(24218, "Read error", "reason"_attr = swBytesRead.getStatus().reason());
        return EIO;
    }

    return 0;
}

int BlockstoreFileSystem::rename(const char* from, const char* to) {
    auto search = _files.find(from);
    if (search == _files.end()) {
        LOGV2_ERROR(24221,
                    "Cannot find file {from}. Renaming it to {to} failed.",
                    "from"_attr = from,
                    "to"_attr = to);
        return EINVAL;
    }

    BlockstoreHTTP blockstoreHTTP(_apiUri, _snapshotId);
    StatusWith<DataBuilder> swRenameResponse =
        blockstoreHTTP.renameFile(getFileRelativePath(from), getFileRelativePath(to));
    if (!swRenameResponse.isOK()) {
        LOGV2_ERROR(24222,
                    "Bad HTTP response from the ApiServer: {swRenameResponse_getStatus_reason}",
                    "swRenameResponse_getStatus_reason"_attr =
                        swRenameResponse.getStatus().reason());
        return EINVAL;
    }

    struct File file = getFile(from);
    file.filename = getFileRelativePath(to);

    // Updating our internal file map.
    _files.erase(from);
    addFile(file);

    return 0;
}

int BlockstoreFileHandle::write(const void* buf, std::size_t offset, std::size_t length) {
    mongo::ConstDataRange wrappedBuf(reinterpret_cast<const char*>(buf), length);
    auto swWrite = _readerWriter->write(wrappedBuf, offset, length);
    if (!swWrite.isOK()) {
        LOGV2_ERROR(24223,
                    "Failed to write {length} to file: {swWrite_getStatus_reason}",
                    "length"_attr = length,
                    "swWrite_getStatus_reason"_attr = swWrite.getStatus().reason());
        return EIO;
    }

    std::string filename = _readerWriter->getFileName();
    std::string path = _blockstoreFs->getFileAbsolutePathFromRelativePath(filename);

    // Calculate how many new bytes were written based on the offset.
    size_t newBytesWritten = 0;
    if (offset + length > static_cast<size_t>(_fileSize)) {
        newBytesWritten = length + offset - static_cast<size_t>(_fileSize);
    }

    _readerWriter->addToFileSize(newBytesWritten);
    _blockstoreFs->addToFileSize(path, newBytesWritten);
    _fileSize += newBytesWritten;

    return 0;
}

}  // namespace queryable
}  // namespace mongo
