/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/base/data_range.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/jsobj.h"
#include "mongo/stdx/memory.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/mongoutils/str.h"

#include "../blockstore/context.h"
#include "../blockstore/http_client.h"
#include "../blockstore/list_dir.h"
#include "../blockstore/reader.h"
#include "../queryable_mmapv1/queryable_global_options.h"

#include "third_party/wiredtiger/src/include/wiredtiger_ext.h"
#include <wiredtiger.h>

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
        _files[_dbpath + "/" + file.filename] = file;
    }

    bool fileExists(const char* filename) {
        return _files.count(filename) > 0;
    }

    struct File getFile(const char* filename) {
        auto ret = _files.find(filename);
        uassert(ErrorCodes::NoSuchKey,
                str::stream() << "Filename not found. Filename: " << filename,
                ret != _files.end());

        return ret->second;
    }

    std::int64_t getFileSize(const char* filename) {
        return _files[filename].fileSize;
    }

    /**
     * `fileHandle` is an out-parameter
     */
    int open(const char* filename, BlockstoreFileHandle** fileHandle);

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
                         std::unique_ptr<Reader> reader,
                         std::int64_t fileSize,
                         std::int32_t blockSize)
        : _blockstoreFs(blockstoreFs),
          _reader(std::move(reader)),
          _fileSize(fileSize),
          _blockSize(blockSize) {}

    WT_FILE_HANDLE* getWtFileHandle() {
        WT_FILE_HANDLE* ret = static_cast<WT_FILE_HANDLE*>(this);
        invariant((void*)this == (void*)ret);
        return ret;
    }

    std::int64_t getFileSize() const {
        return _fileSize;
    }

    std::int32_t getBlockSize() const {
        return _blockSize;
    }

    /**
     * return 0 on success, non-zero on error. As per WT expectations.
     */
    int read(void* buf, std::size_t offset, std::size_t length);

private:
    BlockstoreFileSystem* _blockstoreFs;
    std::unique_ptr<Reader> _reader;
    std::int64_t _fileSize;
    std::int32_t _blockSize;
};

}  // namespace queryable
}  // namespace mongo
