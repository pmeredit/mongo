/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <boost/filesystem.hpp>
#include <memory>
#include <wiredtiger.h>
#include <wiredtiger_ext.h>

#include "../blockstore/context.h"
#include "../blockstore/list_dir.h"
#include "../blockstore/reader_writer.h"
#include "mongo/base/data_range.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/jsobj.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/str.h"
#include "queryable_global_options.h"


extern "C" int queryableWtFsDirectoryList(WT_FILE_SYSTEM* file_system,
                                          WT_SESSION* session,
                                          const char* directory,
                                          const char* prefix,
                                          char*** dirlistp,
                                          uint32_t* countp);

extern "C" int queryableWtFsDirectoryListFree(WT_FILE_SYSTEM* file_system,
                                              WT_SESSION* session,
                                              char** dirlist,
                                              uint32_t count);

namespace mongo {
namespace queryable {

class BlockstoreFileHandle;

class BlockstoreFileSystem : private WT_FILE_SYSTEM {
public:
    BlockstoreFileSystem(std::string apiUri,
                         mongo::OID snapshotId,
                         std::string dbpath,
                         WT_EXTENSION_API* wtExt)
        : _apiUri(std::move(apiUri)),
          _snapshotId(snapshotId),
          _dbpath(std::move(dbpath)),
          _wtExt(wtExt) {}

    WT_FILE_SYSTEM* getWtFileSystem() {
        WT_FILE_SYSTEM* ret = static_cast<WT_FILE_SYSTEM*>(this);
        invariant((void*)this == (void*)ret);
        return ret;
    }

    const std::string& getApiUri() const {
        return _apiUri;
    }

    mongo::OID getSnapshotId() const {
        return _snapshotId;
    }

    WT_EXTENSION_API* getWtExtension() {
        return _wtExt;
    }

    void addFile(struct File file) {
        _files[getFileAbsolutePathFromRelativePath(file.filename)] = file;
    }

    bool fileExists(const char* filename) {
        return _files.count(filename) > 0;
    }

    /**
     * Given _dbPath = "/data/db" and path = "/data/db/journal/WiredTigerLog.001", this returns
     * "journal/WiredTigerLog.001".
     */
    std::string getFileRelativePath(const std::string& path) {
        boost::filesystem::path dbPath(_dbpath);
        boost::filesystem::path fullPath(path);

        boost::filesystem::path relativePath = boost::filesystem::relative(fullPath, dbPath);
        return relativePath.string();
    }

    /**
     * Given _dbPath = "/data/db" and relativePath = "journal/WiredTigerLog.001", this returns
     * "/data/db/journal/WiredTigerLog.001".
     */
    std::string getFileAbsolutePathFromRelativePath(const std::string& relativePath) {
        boost::filesystem::path fullPath = _dbpath;
        fullPath /= relativePath;
        return fullPath.string();
    }

    struct File getFile(const char* filename) {
        auto ret = _files.find(filename);
        uassert(ErrorCodes::NoSuchKey,
                str::stream() << "Filename not found. Filename: " << filename,
                ret != _files.end());

        return ret->second;
    }

    std::int64_t getFileSize(const std::string& filename) {
        return _files[filename].fileSize;
    }

    void addToFileSize(const std::string& filename, size_t sizeToAdd) {
        _files[filename].fileSize += sizeToAdd;
    }

    /**
     * Returns a list of files that match 'prefix' in 'directory'.
     */
    std::vector<std::string> getFiles(const char* directory, const char* prefix) {
        boost::filesystem::path directoryPath = directory;
        const std::string prefixToMatch(prefix);

        std::vector<std::string> files;
        if (directoryPath.string().rfind(_dbpath, 0) != 0) {
            // Cannot search outside of db path;
            return files;
        }

        for (const auto& file : _files) {
            if (file.first.rfind(directoryPath.string(), 0) != 0) {
                // File not contained in directory.
                continue;
            }

            boost::filesystem::path fullPath = file.first;
            boost::filesystem::path relativePath =
                boost::filesystem::relative(fullPath, directoryPath);

            auto relativePathIt = relativePath.begin();
            if (relativePathIt == relativePath.end()) {
                continue;
            }

            std::string fileName = relativePathIt->string();

            if (prefixToMatch.size() == 0) {
                files.push_back(fileName);
                continue;
            }

            if (fileName.rfind(prefixToMatch, 0) != 0) {
                continue;
            }

            files.push_back(fileName);
        }
        return files;
    }

    /**
     * `fileHandle` is an out-parameter
     */
    int open(const char* filename, uint32_t flags, BlockstoreFileHandle** fileHandle);

    int rename(const char* from, const char* to);

private:
    std::string _apiUri;
    mongo::OID _snapshotId;
    stdx::unordered_map<std::string, struct File> _files;
    std::string _dbpath;
    WT_EXTENSION_API* _wtExt; /* Extension functions, e.g outputting errors */
};

class BlockstoreFileHandle : private WT_FILE_HANDLE {
public:
    BlockstoreFileHandle(BlockstoreFileSystem* blockstoreFs,
                         std::unique_ptr<ReaderWriter> readerWriter,
                         std::int64_t fileSize)
        : _blockstoreFs(blockstoreFs),
          _readerWriter(std::move(readerWriter)),
          _fileSize(fileSize) {}

    WT_FILE_HANDLE* getWtFileHandle() {
        WT_FILE_HANDLE* ret = static_cast<WT_FILE_HANDLE*>(this);
        invariant((void*)this == (void*)ret);
        return ret;
    }

    std::int64_t getFileSize() const {
        return _fileSize;
    }

    /**
     * return 0 on success, non-zero on error. As per WT expectations.
     */
    int read(void* buf, std::size_t offset, std::size_t length);
    int write(const void* buf, std::size_t offset, std::size_t length);

private:
    BlockstoreFileSystem* _blockstoreFs;
    std::unique_ptr<ReaderWriter> _readerWriter;
    std::int64_t _fileSize;
};

}  // namespace queryable
}  // namespace mongo
