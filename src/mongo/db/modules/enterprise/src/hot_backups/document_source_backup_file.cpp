/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */


#include "document_source_backup_file.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include <algorithm>
#include <boost/filesystem.hpp>

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage


namespace mongo {

REGISTER_INTERNAL_DOCUMENT_SOURCE(_backupFile,
                                  LiteParsedDocumentSourceDefault::parse,
                                  DocumentSourceBackupFile::createFromBson,
                                  true);

boost::intrusive_ptr<DocumentSourceBackupFile> DocumentSourceBackupFile::create(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, DocumentSourceBackupFileSpec spec) {
    return make_intrusive<DocumentSourceBackupFile>(expCtx, std::move(spec));
}

boost::intrusive_ptr<DocumentSourceBackupFile> DocumentSourceBackupFile::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {

    uassert(ErrorCodes::FailedToParse,
            str::stream() << kStageName
                          << " value must be an object. Found: " << typeName(elem.type()),
            elem.type() == BSONType::Object);


    auto spec = DocumentSourceBackupFileSpec::parse(IDLParserErrorContext(kStageName),
                                                    elem.embeddedObject());


    auto svcCtx = expCtx->opCtx->getServiceContext();
    auto backupCursorService = BackupCursorHooks::get(svcCtx);

    auto backupId = spec.getBackupId();
    auto fileName = spec.getFile().toString();

    uassert(ErrorCodes::IllegalOperation,
            str::stream() << "File " << fileName
                          << " must be returned by the active backup cursor with backup ID "
                          << backupId,
            backupCursorService->isFileReturnedByCursor(backupId, fileName));
    return DocumentSourceBackupFile::create(expCtx, spec);
}

DocumentSource::GetNextResult DocumentSourceBackupFile::doGetNext() {
#ifdef _WIN32
    // On Windows, it is vital that we close the file when we're not using it to avoid a crash when
    // WiredTiger attempts to delete the file on shutdown or when the backup is done.  Closing the
    // file would make performance terrible in the general case, so we only do it when not in
    // exhaust mode (e.g. on the find command, since we don't start exhaust until the first
    // getMore).  When we are in exhaust mode, we're guaranteed that breaking the connection kills
    // the cursor, which will call doDispose and close the file there.
    //
    // We always re-open the file if it was closed and seek to '_offset', so we
    // don't need to do anything special to handle that.
    ON_BLOCK_EXIT([this] {
        const bool isExhaust = getContext()->opCtx->isExhaust();
        LOGV2_DEBUG(6170800,
                    0,
                    "Possibly closing file at end of getNext",
                    "path"_attr = _backupFileSpec.getFile().toString(),
                    "isExhaust"_attr = isExhaust,
                    "eof"_attr = _eof,
                    "remainingLength"_attr = _remainingLengthToRead,
                    "open"_attr = _src.is_open());
        if (!isExhaust) {
            _src.close();
            // Ignore close errors; the file may have already been closed.
            _src.clear();
        }
    });
#endif
    // If we have reached the end of file or read up to the desired length, return EOF to signal
    // that this document source is exhausted.
    if (_eof || _remainingLengthToRead == 0) {
        _src.close();
        return GetNextResult::makeEOF();
    }

    auto isLengthSpecified = _backupFileSpec.getLength() != -1;
    auto lengthToRead = isLengthSpecified
        ? std::min(_remainingLengthToRead, static_cast<std::streamsize>(BSONObjMaxUserSize - 1))
        : static_cast<std::streamsize>(BSONObjMaxUserSize - 1);

    BSONObjBuilder builder;
    auto path = _backupFileSpec.getFile().toString();

    if (!_src.is_open()) {
        _src.open(path, std::ios_base::in | std::ios_base::binary);
        uassert(ErrorCodes::FileNotOpen,
                str::stream() << "File " << path << " failed to open",
                _src.is_open());
    }

    std::vector<char> buf(lengthToRead);

    // If there is a byte offset specified, we set the starting position of the stream to the
    // offset.
    _src.seekg(_offset);
    _src.read(buf.data(), lengthToRead);

    if (_src.eof()) {
        _eof = true;
    } else {
        uassert(ErrorCodes::FileStreamFailed,
                str::stream() << "Reading operation from file " << path << " failed",
                !_src.fail());
        if (isLengthSpecified)
            _remainingLengthToRead -= _src.gcount();
    }

    builder.append("byteOffset", _offset);
    if (_eof) {
        builder.appendBool("endOfFile", _eof);
    }
    builder.appendBinData("data", _src.gcount(), BinDataGeneral, buf.data());

    if (!_eof) {
        _offset = _src.tellg();
    } else {
        _src.close();
    }

    return {Document{builder.obj()}};
}

void DocumentSourceBackupFile::doDispose() {
    _eof = true;
    _src.close();
}
DocumentSourceBackupFile::DocumentSourceBackupFile(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, DocumentSourceBackupFileSpec spec)
    : DocumentSource(kStageName, expCtx),
      _backupFileSpec(std::move(spec)),
      _offset(spec.getByteOffset()),
      _remainingLengthToRead(_backupFileSpec.getLength()) {}

Value DocumentSourceBackupFile::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value();
}

}  // namespace mongo
