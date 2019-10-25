/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "document_source_backup_cursor.h"

#include <vector>

#include "mongo/bson/bsonmisc.h"
#include "mongo/util/log.h"

namespace mongo {

REGISTER_DOCUMENT_SOURCE(backupCursor,
                         DocumentSourceBackupCursor::LiteParsed::parse,
                         DocumentSourceBackupCursor::createFromBson);

DocumentSourceBackupCursor::DocumentSourceBackupCursor(
    const boost::intrusive_ptr<ExpressionContext>& pExpCtx)
    : DocumentSource(kStageName, pExpCtx),
      _backupCursorState(pExpCtx->mongoProcessInterface->openBackupCursor(pExpCtx->opCtx)) {}

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

    return new DocumentSourceBackupCursor(pExpCtx);
}

Value DocumentSourceBackupCursor::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(BSON(kStageName << 1));
}
}  // namespace mongo
