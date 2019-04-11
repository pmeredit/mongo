/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <fcntl.h>
#ifndef _WIN32
#include <sys/mman.h>
#endif
#include <sys/stat.h>
#include <sys/types.h>
#ifndef _WIN32
#include <unistd.h>
#endif

#include "document_source_bson_file.h"

#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/util/errno_util.h"

namespace mongo {

using boost::intrusive_ptr;
using std::deque;

DocumentSourceBSONFile::DocumentSourceBSONFile(const intrusive_ptr<ExpressionContext>& pCtx,
                                               const char* fileName)
    : DocumentSource(pCtx),
      _fileName(fileName),
#ifdef _WIN32
      _mapped(nullptr) {
#else
      _mapped(MAP_FAILED) {
#endif
#ifdef _WIN32
    _file = CreateFileA(
        _fileName.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, 0L, nullptr);
    uassert(ErrorCodes::FileNotOpen,
            str::stream() << "Failed to open " << _fileName << ": "
                          << errnoWithDescription(GetLastError()),
            _file != INVALID_HANDLE_VALUE);

    LARGE_INTEGER liFileSize;
    uassert(ErrorCodes::FileNotOpen,
            str::stream() << "Failed to stat " << _fileName << ": "
                          << errnoWithDescription(GetLastError()),
            GetFileSizeEx(_file, &liFileSize));

    _fileSize = static_cast<size_t>(liFileSize.QuadPart);

    _fileMapping = CreateFileMappingA(_file, nullptr, PAGE_READONLY, 0L, 0L, nullptr);
    uassert(ErrorCodes::FileNotOpen,
            str::stream() << "Failed to mmap " << _fileName << ": "
                          << errnoWithDescription(GetLastError()),
            _fileMapping != nullptr);

    _mapped = MapViewOfFile(_fileMapping, FILE_MAP_READ, 0L, 0L, 0L);
    uassert(ErrorCodes::FileNotOpen,
            str::stream() << "Failed to mmap " << _fileName << ": "
                          << errnoWithDescription(GetLastError()),
            _mapped != nullptr);
#else
    _fd = open(fileName, O_RDONLY);
    uassert(ErrorCodes::FileNotOpen,
            str::stream() << "Failed to open " << _fileName << ": " << strerror(errno),
            _fd >= 0);

    struct stat buf;
    uassert(ErrorCodes::FileNotOpen,
            str::stream() << "Failed to stat " << _fileName << ": " << strerror(errno),
            fstat(_fd, &buf) == 0);

    _fileSize = buf.st_size;

    _mapped = mmap(nullptr, _fileSize, PROT_READ, MAP_SHARED, _fd, 0);
    uassert(ErrorCodes::FileNotOpen,
            str::stream() << "Failed to mmap " << _fileName << ": " << strerror(errno),
            _mapped != MAP_FAILED);
#endif
}

DocumentSourceBSONFile::~DocumentSourceBSONFile() {
#ifdef _WIN32
    if (_mapped != nullptr) {
        UnmapViewOfFile(_mapped);
        _mapped = nullptr;
    }

    if (_fileMapping != nullptr) {
        CloseHandle(_fileMapping);
        _fileMapping = nullptr;
    }

    if (_file != nullptr && _file != INVALID_HANDLE_VALUE) {
        CloseHandle(_file);
        _file = nullptr;
    }
#else
    if (_mapped != MAP_FAILED) {
        (void)munmap(_mapped, _fileSize);
        _mapped = MAP_FAILED;
    }

    if (_fd >= 0) {
        (void)close(_fd);
    }
#endif
}

const char* DocumentSourceBSONFile::getSourceName() const {
    return "BSONFile";
}

Value DocumentSourceBSONFile::serialize(boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(Document{{getSourceName(), Document()}});
}

void DocumentSourceBSONFile::doDispose() {
    isDisposed = true;
}

intrusive_ptr<DocumentSourceBSONFile> DocumentSourceBSONFile::create(
    const intrusive_ptr<ExpressionContext>& pCtx, const char* fileName) {
    return new DocumentSourceBSONFile(pCtx, fileName);
}

DocumentSource::GetNextResult DocumentSourceBSONFile::getNext() {
    if (static_cast<size_t>(_offset) == _fileSize) {
        return GetNextResult::makeEOF();
    }

    uassert(ErrorCodes::BadValue,
            str::stream() << "Partial object at end of " << _fileName,
            static_cast<off_t>(_fileSize) - _offset >= 4);

    BSONObj obj(static_cast<const char*>(_mapped) + _offset);
    uassert(ErrorCodes::BadValue,
            str::stream() << "Partial object at end of " << _fileName,
            static_cast<off_t>(_fileSize) - _offset >= obj.objsize());

    _offset += obj.objsize();
    return GetNextResult(Document(obj));
}

}  // namespace mongo
