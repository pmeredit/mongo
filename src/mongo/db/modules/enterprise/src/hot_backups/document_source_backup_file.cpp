/**
 * Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <algorithm>
#include <boost/filesystem.hpp>

#include "document_source_backup_file.h"

#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage


namespace mongo {
class LiteParsedDocumentSourceBackupFile : public LiteParsedDocumentSource {
public:
    static std::unique_ptr<LiteParsedDocumentSourceBackupFile> parse(const NamespaceString& nss,
                                                                     const BSONElement& spec) {
        return std::make_unique<LiteParsedDocumentSourceBackupFile>(spec.fieldName());
    }

    // Backup files should only be available to authenticated cluster members.
    LiteParsedDocumentSourceBackupFile(std::string parseTimeName)
        : LiteParsedDocumentSource(std::move(parseTimeName)),
          _privileges({Privilege(ResourcePattern::forClusterResource(boost::none),
                                 ActionSet{ActionType::internal})}) {}

    stdx::unordered_set<NamespaceString> getInvolvedNamespaces() const final {
        return stdx::unordered_set<NamespaceString>();
    }

    PrivilegeVector requiredPrivileges(bool isMongos, bool bypassDocumentValidation) const final {
        return _privileges;
    }

    // Declaring the document source as "isInitialSource" means the db it is called with does
    // not figure into the authorization.
    bool isInitialSource() const final {
        return true;
    }

private:
    const PrivilegeVector _privileges;
};

REGISTER_INTERNAL_DOCUMENT_SOURCE(_backupFile,
                                  LiteParsedDocumentSourceBackupFile::parse,
                                  DocumentSourceBackupFile::createFromBson,
                                  true);
ALLOCATE_DOCUMENT_SOURCE_ID(_backupFile, DocumentSourceBackupFile::id)

boost::intrusive_ptr<DocumentSourceBackupFile> DocumentSourceBackupFile::create(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, DocumentSourceBackupFileSpec spec) {
    return make_intrusive<DocumentSourceBackupFile>(expCtx, std::move(spec));
}

boost::intrusive_ptr<DocumentSourceBackupFile> DocumentSourceBackupFile::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {

    const NamespaceString& nss = expCtx->getNamespaceString();
    uassert(ErrorCodes::InvalidNamespace,
            "$_backupFile must be run against the 'admin' database with {aggregate: 1}",
            nss.isAdminDB() && nss.isCollectionlessAggregateNS());

    uassert(ErrorCodes::FailedToParse,
            str::stream() << kStageName
                          << " value must be an object. Found: " << typeName(elem.type()),
            elem.type() == BSONType::Object);

    auto spec =
        DocumentSourceBackupFileSpec::parse(IDLParserContext(kStageName), elem.embeddedObject());

    return DocumentSourceBackupFile::create(expCtx, spec);
}

bool DocumentSourceBackupFile::backupSessionIsValid() const {
    auto svcCtx = pExpCtx->getOperationContext()->getServiceContext();
    auto backupCursorService = BackupCursorHooks::get(svcCtx);

    auto backupId = _backupFileSpec.getBackupId();
    auto filePath = _backupFileSpec.getFile().toString();

    return backupCursorService->isFileReturnedByCursor(backupId, filePath);
}

void DocumentSourceBackupFile::checkBackupSessionStillValid() const {
    uassert(7124700,
            str::stream() << "Backup with ID " << _backupFileSpec.getBackupId()
                          << "was closed while reading backup file " << _backupFileSpec.getFile(),
            backupSessionIsValid());
}

void DocumentSourceBackupFile::prepareForExecution() {
    if (_execState == ExecState::kUninitialized) {
        uassert(ErrorCodes::IllegalOperation,
                str::stream() << "File " << _backupFileSpec.getFile()
                              << " must be returned by the active backup cursor with backup ID "
                              << _backupFileSpec.getBackupId(),
                backupSessionIsValid());
        _execState = ExecState::kActive;
    }
}

DocumentSource::GetNextResult DocumentSourceBackupFile::doGetNext() {
    prepareForExecution();

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
        const bool isExhaust = getContext()->getOperationContext()->isExhaust();
        LOGV2_DEBUG(6170800,
                    1,
                    "Possibly closing file at end of getNext",
                    "path"_attr = _backupFileSpec.getFile().toString(),
                    "isExhaust"_attr = isExhaust,
                    "eof"_attr = isEof(_execState),
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
    if (isEof(_execState) || _remainingLengthToRead == 0) {
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

    // After we read, we must ensure the backup session still exists; otherwise, WiredTiger
    // might have started writing to our file and we may have read invalid data.
    // We do this before setting _execState or adjusting _remainingLength to ensure subsequent calls
    // don't return an apparently-valid EOF.
    checkBackupSessionStillValid();

    if (_src.eof()) {
        _execState = ExecState::kEof;
    } else {
        uassert(ErrorCodes::FileStreamFailed,
                str::stream() << "Reading operation from file " << path << " failed",
                !_src.fail());
        if (isLengthSpecified)
            _remainingLengthToRead -= _src.gcount();
    }

    builder.append("byteOffset", _offset);
    if (isEof(_execState)) {
        builder.appendBool("endOfFile", true);
    }
    builder.appendBinData("data", _src.gcount(), BinDataGeneral, buf.data());

    if (isEof(_execState)) {
        _src.close();
    } else {
        invariant(_execState == ExecState::kActive);
        _offset = _src.tellg();
    }

    return Document{builder.obj()};
}

void DocumentSourceBackupFile::doDispose() {
    _execState = ExecState::kEof;
    _src.close();
}
DocumentSourceBackupFile::DocumentSourceBackupFile(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, DocumentSourceBackupFileSpec spec)
    : DocumentSource(kStageName, expCtx),
      _backupFileSpec(std::move(spec)),
      _offset(_backupFileSpec.getByteOffset()),
      _remainingLengthToRead(_backupFileSpec.getLength()) {}

Value DocumentSourceBackupFile::serialize(const SerializationOptions& opts) const {
    if (opts.literalPolicy != LiteralSerializationPolicy::kToRepresentativeParseableValue) {
        return Value(Document{{kStageName, _backupFileSpec.toBSON(opts)}});
    }

    // For a representative query shape, we need some values which will be accepted by the parser.
    // Thus we need a valid UUID - and the same one each time, so we can't just call UUID::gen().
    static constexpr auto representativeUUID = "00000000-0000-4000-8000-000000000000"_sd;
    static const auto uuidRes = UUID::parse(representativeUUID);
    uassertStatusOK(uuidRes);
    auto specCopy = _backupFileSpec;
    specCopy.setBackupId(uuidRes.getValue());
    specCopy.setFile("?");
    specCopy.setByteOffset(1);
    specCopy.setLength(1);
    return Value(Document{{kStageName, specCopy.toBSON()}});
}

}  // namespace mongo
