/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include <fcntl.h>
#include <fmt/format.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifndef _WIN32
#include <sys/mman.h>
#include <unistd.h>
#endif

#include "document_source_bson_file.h"

#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/util/errno_util.h"

namespace mongo {

using boost::intrusive_ptr;

DocumentSourceBSONFile::DocumentSourceBSONFile(const intrusive_ptr<ExpressionContext>& pCtx,
                                               StringData fileName)
    : DocumentSource(kStageName, pCtx),
      _fileName(fileName),
#ifdef _WIN32
      _mapped(nullptr) {
#else
      _mapped(MAP_FAILED) {
#endif
    using namespace fmt::literals;
    auto errMsg = [&](StringData op) {
        auto ec = lastSystemError();
        return "Failed to {} {}: {}"_format(op, _fileName, errorMessage(ec));
    };

#ifdef _WIN32
    _file = CreateFileA(
        _fileName.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, 0L, nullptr);
    uassert(ErrorCodes::FileNotOpen, errMsg("open"), _file != INVALID_HANDLE_VALUE);

    LARGE_INTEGER liFileSize;
    uassert(ErrorCodes::FileNotOpen, errMsg("stat"), GetFileSizeEx(_file, &liFileSize));

    _fileSize = static_cast<size_t>(liFileSize.QuadPart);

    _fileMapping = CreateFileMappingA(_file, nullptr, PAGE_READONLY, 0L, 0L, nullptr);
    uassert(ErrorCodes::FileNotOpen, errMsg("mmap"), _fileMapping != nullptr);

    _mapped = MapViewOfFile(_fileMapping, FILE_MAP_READ, 0L, 0L, 0L);
    uassert(ErrorCodes::FileNotOpen, errMsg("mmap"), _mapped != nullptr);
#else
    _fd = open(_fileName.c_str(), O_RDONLY);
    uassert(ErrorCodes::FileNotOpen, errMsg("open"), _fd >= 0);

    struct stat buf;
    uassert(ErrorCodes::FileNotOpen, errMsg("stat"), fstat(_fd, &buf) == 0);

    _fileSize = buf.st_size;

    _mapped = mmap(nullptr, _fileSize, PROT_READ, MAP_SHARED, _fd, 0);
    uassert(ErrorCodes::FileNotOpen, errMsg("mmap"), _mapped != MAP_FAILED);
#endif
}

DocumentSourceBSONFile::~DocumentSourceBSONFile() {
    // The owner of a DocumentSource* object should call dispose() on it before destroying it.
    invariant(_fileSize == 0);
    invariant(_offset == 0);
}

void DocumentSourceBSONFile::doDispose() {
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

    // This will ensure that future getNext() calls return EOF.
    _fileSize = 0;
    _offset = 0;
}

const char* DocumentSourceBSONFile::getSourceName() const {
    return kStageName.rawData();
}

Value DocumentSourceBSONFile::serialize(SerializationOptions opts) const {
    return Value(Document{{getSourceName(), opts.serializeLiteralValue(Value(_fileName))}});
}

intrusive_ptr<DocumentSourceBSONFile> DocumentSourceBSONFile::create(
    const intrusive_ptr<ExpressionContext>& pExpCtx, StringData fileName) {
    return new DocumentSourceBSONFile(pExpCtx, fileName);
}

DocumentSource::GetNextResult DocumentSourceBSONFile::doGetNext() {
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
    return GetNextResult(Document(obj.getOwned()));
}

}  // namespace mongo
