/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#include "document_source_backup_file.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include <algorithm>
#include <boost/filesystem.hpp>

namespace mongo {

REGISTER_DOCUMENT_SOURCE_CONDITIONALLY(
    _backupFile,
    LiteParsedDocumentSourceDefault::parse,
    DocumentSourceBackupFile::createFromBson,
    AllowedWithApiStrict::kInternal,
    AllowedWithClientType::kInternal,
    boost::none,
    repl::feature_flags::gFileCopyBasedInitialSync.isEnabledAndIgnoreFCV());

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
