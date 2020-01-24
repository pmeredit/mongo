/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "document_source_backup_cursor.h"

#include <vector>

#include "mongo/bson/bsonmisc.h"
#include "mongo/util/log.h"

#include "hot_backups/backup_cursor_parameters_gen.h"

namespace mongo {

REGISTER_DOCUMENT_SOURCE(backupCursor,
                         DocumentSourceBackupCursor::LiteParsed::parse,
                         DocumentSourceBackupCursor::createFromBson);

DocumentSourceBackupCursor::DocumentSourceBackupCursor(
    const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
    const StorageEngine::BackupOptions& options)
    : DocumentSource(kStageName, pExpCtx),
      _backupCursorState(
          pExpCtx->mongoProcessInterface->openBackupCursor(pExpCtx->opCtx, options)) {}

DocumentSourceBackupCursor::~DocumentSourceBackupCursor() {
    try {
        pExpCtx->mongoProcessInterface->closeBackupCursor(pExpCtx->opCtx,
                                                          _backupCursorState.backupId);
    } catch (DBException& exc) {
        severe() << exc.toStatus("Error closing a backup cursor.");
        fassertFailed(50909);
    }
}

DocumentSource::GetNextResult DocumentSourceBackupCursor::doGetNext() {
    if (_backupCursorState.preamble) {
        Document doc = _backupCursorState.preamble.get();
        _backupCursorState.preamble = boost::none;

        return std::move(doc);
    }

    if (!_backupCursorState.blocksToCopy.empty()) {
        const StorageEngine::BackupBlock& block = _backupCursorState.blocksToCopy.back();
        Document doc = {{"filename", block.filename},
                        {"offset", static_cast<long long>(block.offset)},
                        {"length", static_cast<long long>(block.length)}};
        _backupCursorState.blocksToCopy.pop_back();

        return std::move(doc);
    }

    return GetNextResult::makeEOF();
}

boost::intrusive_ptr<DocumentSource> DocumentSourceBackupCursor::createFromBson(
    BSONElement spec, const boost::intrusive_ptr<ExpressionContext>& pExpCtx) {
    // The anticipated usage of a backup cursor: open the backup cursor, consume the results, copy
    // data off disk, close the backup cursor. The backup cursor must be successfully closed for
    // the data copied to be valid. Hence, the caller needs a way to keep the cursor open after
    // consuming the results, as well as the ability to send "heartbeats" to prevent the client
    // cursor manager from timing out the backup cursor. A backup cursor does consume resources;
    // in the event the calling process crashes, the cursors should eventually be timed out.
    pExpCtx->tailableMode = TailableModeEnum::kTailable;

    uassert(ErrorCodes::FailedToParse,
            str::stream() << kStageName
                          << " value must be an object. Found: " << typeName(spec.type()),
            spec.type() == BSONType::Object);

    uassert(ErrorCodes::CannotBackup,
            str::stream() << kStageName << " cannot be executed against a MongoS.",
            !pExpCtx->inMongos && !pExpCtx->fromMongos && !pExpCtx->needsMerge);

    // Parse $backupCursor arguments for incremental backups.
    BackupCursorParameters params =
        BackupCursorParameters::parse(IDLParserErrorContext(""), spec.Obj());

    StorageEngine::BackupOptions options;

    options.disableIncrementalBackup = params.getDisableIncrementalBackup();
    options.incrementalBackup = params.getIncrementalBackup();
    options.blockSizeMB = params.getBlockSizeMB();
    if (params.getThisBackupName()) {
        options.thisBackupName.emplace(params.getThisBackupName()->toString());
    }
    if (params.getSrcBackupName()) {
        options.srcBackupName.emplace(params.getSrcBackupName()->toString());
    }

    if (options.disableIncrementalBackup) {
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "Cannot have 'thisBackupName' or 'srcBackupName' when "
                              << "disableIncrementalBackup=false",
                !options.thisBackupName && !options.srcBackupName);
    }

    if (!options.incrementalBackup) {
        uassert(
            ErrorCodes::InvalidOptions,
            str::stream()
                << "Cannot have 'thisBackupName' or 'srcBackupName' when incrementalBackup=false",
            !options.thisBackupName && !options.srcBackupName);
    } else {
        uassert(ErrorCodes::InvalidOptions,
                "Cannot specify both 'incrementalBackup' and 'disableIncrementalBackup' as true.",
                !options.disableIncrementalBackup);
        uassert(ErrorCodes::InvalidOptions,
                "'thisBackupName' needs to exist when incrementalBackup=true",
                options.thisBackupName);
        uassert(ErrorCodes::InvalidOptions,
                "'thisBackupName' needs to be a non-empty string",
                !options.thisBackupName->empty());
        if (options.srcBackupName) {
            uassert(ErrorCodes::InvalidOptions,
                    "'srcBackupName' needs to be a non-empty string",
                    !options.srcBackupName->empty());
        }
    }

    return new DocumentSourceBackupCursor(pExpCtx, options);
}

Value DocumentSourceBackupCursor::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(BSON(kStageName << 1));
}
}  // namespace mongo
