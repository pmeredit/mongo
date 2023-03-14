/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#ifndef _WIN32
#include <cstdio>
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
    auto n = std::fread(buf, 1, count, stdin);

    if (n > 0)
        return n;

    if (std::feof(stdin))
        return 0;

    return -1;
#endif
}

}  // namespace

DocumentSourceStdin::DocumentSourceStdin(const boost::intrusive_ptr<ExpressionContext>& pExpCtx)
    : DocumentSource(DocumentSourceStdin::kStageName, pExpCtx) {

#ifndef _WIN32
    // Reopen stdin in binary mode.
    FILE* fh = std::freopen(nullptr, "rb", stdin);
    uassert(ErrorCodes::FileNotOpen,
            str::stream() << "Failed to reopen stdin for binary data: " << strerror(errno),
            fh != nullptr);

    // Set buffer size to default pipe buffer size on Linux.
    int ok = std::setvbuf(stdin, nullptr, _IOFBF, 65536);
    uassert(ErrorCodes::FileNotOpen,
            str::stream() << "Failed to resize stdin buffer: " << strerror(errno),
            ok == 0);
#endif
}

DocumentSource::GetNextResult DocumentSourceStdin::doGetNext() {
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
    return GetNextResult(Document(obj.getOwned()));
}

const char* DocumentSourceStdin::getSourceName() const {
    return kStageName.rawData();
}

Value DocumentSourceStdin::serialize(SerializationOptions opts) const {
    if (opts.redactFieldNames || opts.replacementForLiteralArgs) {
        MONGO_UNIMPLEMENTED_TASSERT(7484369);
    }
    return Value(Document{{getSourceName(), Document()}});
}

boost::intrusive_ptr<DocumentSourceStdin> DocumentSourceStdin::create(
    const boost::intrusive_ptr<ExpressionContext>& pExpCtx) {
    return new DocumentSourceStdin(pExpCtx);
}

}  // namespace mongo
