/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#ifndef _WIN32
#include <stdio.h>  // For STDIN_FILENO
#include <unistd.h>
#endif

#include "document_source_stdin.h"

#ifdef _WIN32
typedef SSIZE_T ssize_t;
#endif

namespace mongo {

namespace {

ssize_t readStdin(void* buf, size_t count) {
#ifdef _WIN32
    unsigned long bytesRead;
    HANDLE hFile = GetStdHandle(STD_INPUT_HANDLE);
    if (!ReadFile(hFile, buf, static_cast<unsigned long>(count), &bytesRead, nullptr)) {
        return GetLastError() == ERROR_BROKEN_PIPE ? 0 : -1;
    }
    return static_cast<ssize_t>(bytesRead);
#else
    return read(STDIN_FILENO, buf, count);
#endif
}

}  // namespace

DocumentSource::GetNextResult DocumentSourceStdin::getNext() {
    char sizeBuf[4];
    auto n = readStdin(&sizeBuf, sizeof(sizeBuf));
    if (n == 0) {
        return GetNextResult::makeEOF();
    } else if (n != sizeof(sizeBuf)) {
        std::cerr << "error reading from stdin" << std::endl;
        return GetNextResult::makeEOF();
    }

    uint32_t size = ConstDataView(sizeBuf).read<LittleEndian<std::uint32_t>>();
    uassert(30172,
            str::stream() << "Input BSON document has size " << size
                          << ", which exceeds maximum allowed object size",
            size < BSONObjMaxInternalSize);
    auto buf = SharedBuffer::allocate(size);
    invariant(buf.get());

    memcpy(buf.get(), sizeBuf, sizeof(sizeBuf));
    size_t totalRead = sizeof(sizeBuf);
    while ((n = readStdin(buf.get() + totalRead, size - totalRead)) > 0) {
        totalRead += n;
        if (totalRead == size) {
            break;
        }
    }
    if (n < 0) {
        std::cerr << "error reading from stdin" << std::endl;
        return GetNextResult::makeEOF();
    }
    if (n == 0) {
        invariant(totalRead < size);
        std::cerr << "unexpected end of document" << std::endl;
        return GetNextResult::makeEOF();
    }
    invariant(totalRead == size);

    BSONObj obj(buf);
    return GetNextResult(Document(obj));
}

const char* DocumentSourceStdin::getSourceName() const {
    return "stdin";
}

Value DocumentSourceStdin::serialize(boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(Document{{getSourceName(), Document()}});
}

boost::intrusive_ptr<DocumentSourceStdin> DocumentSourceStdin::create(
    const boost::intrusive_ptr<ExpressionContext>& pExpCtx) {
    return new DocumentSourceStdin(pExpCtx);
}

}  // namespace mongo
