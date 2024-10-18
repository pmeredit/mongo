/**
 * Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"
#include "mongo/util/testing_proctor.h"

#include "document_source_backup_cursor.h"

#include <vector>

#include "mongo/bson/bsonmisc.h"
#include "mongo/db/s/replica_set_endpoint_feature_flag.h"
#include "mongo/logv2/log.h"

#include "hot_backups/backup_cursor_parameters_gen.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery


namespace mongo {

REGISTER_DOCUMENT_SOURCE(backupCursor,
                         DocumentSourceBackupCursor::LiteParsed::parse,
                         DocumentSourceBackupCursor::createFromBson,
                         AllowedWithApiStrict::kAlways);

DocumentSourceBackupCursor::DocumentSourceBackupCursor(
    const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
    BackupCursorParameters params,
    const StorageEngine::BackupOptions& options)
    : DocumentSource(kStageName, pExpCtx),
      _params(std::move(params)),
      _backupCursorState(pExpCtx->mongoProcessInterface->openBackupCursor(pExpCtx->opCtx, options)),
      _kBatchSize(TestingProctor::instance().isEnabled() ? 10 : 1000) {}

DocumentSourceBackupCursor::~DocumentSourceBackupCursor() {
    try {
        pExpCtx->mongoProcessInterface->closeBackupCursor(pExpCtx->opCtx,
                                                          _backupCursorState.backupId);
    } catch (DBException& exc) {
        LOGV2_FATAL(50909, "Error closing a backup cursor", "error"_attr = exc);
    }
}

DocumentSource::GetNextResult DocumentSourceBackupCursor::doGetNext() {
    if (_backupCursorState.preamble) {
        Document doc = _backupCursorState.preamble.value();
        _backupCursorState.preamble = boost::none;
        return std::move(doc);
    }

    if (!_backupCursorState.otherBackupBlocks.empty()) {
        invariant(_backupBlocks.empty());
        _backupBlocks = std::move(_backupCursorState.otherBackupBlocks);
        _backupCursorState.otherBackupBlocks.clear();
    }

    if (_backupBlocks.empty() && _backupCursorState.streamingCursor) {
        _backupBlocks =
            uassertStatusOK(_backupCursorState.streamingCursor->getNextBatch(_kBatchSize));
    }

    if (!_backupBlocks.empty()) {
        const BackupBlock& backupBlock = _backupBlocks.front();

        if (backupBlock.offset() > static_cast<uint64_t>(std::numeric_limits<long long>::max()) ||
            backupBlock.length() > static_cast<uint64_t>(std::numeric_limits<long long>::max()) ||
            backupBlock.fileSize() > static_cast<uint64_t>(std::numeric_limits<long long>::max())) {
            std::stringstream ss;
            ss << "Offset " << backupBlock.offset() << ", length " << backupBlock.length()
               << ", or " << backupBlock.fileSize()
               << " is too large to convert from uint64_t to long long";
            uasserted(ErrorCodes::Overflow, ss.str());
        }

        std::string uuidStr;
        if (backupBlock.uuid().has_value()) {
            uuidStr = backupBlock.uuid().value().toString();
        }

        Document doc;
        if (backupBlock.offset() == 0 && backupBlock.length() == 0) {
            doc = {{"filename", backupBlock.filePath()},
                   {"fileSize", static_cast<long long>(backupBlock.fileSize())},
                   {"required", backupBlock.isRequired()},
                   {"ns",
                    backupBlock.ns() ? NamespaceStringUtil::serialize(
                                           *backupBlock.ns(), SerializationContext::stateDefault())
                                     : ""},
                   {"uuid", uuidStr}};
        } else {
            doc = {{"filename", backupBlock.filePath()},
                   {"fileSize", static_cast<long long>(backupBlock.fileSize())},
                   {"offset", static_cast<long long>(backupBlock.offset())},
                   {"length", static_cast<long long>(backupBlock.length())},
                   {"required", backupBlock.isRequired()},
                   {"ns",
                    backupBlock.ns() ? NamespaceStringUtil::serialize(
                                           *backupBlock.ns(), SerializationContext::stateDefault())
                                     : ""},
                   {"uuid", uuidStr}};
        }

        auto svcCtx = pExpCtx->opCtx->getServiceContext();
        auto backupCursorService = BackupCursorHooks::get(svcCtx);
        backupCursorService->addFile(_backupCursorState.backupId, backupBlock.filePath());

        _backupBlocks.pop_front();
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

    if (replica_set_endpoint::isFeatureFlagEnabled()) {
        uassert(ErrorCodes::InvalidNamespace,
                str::stream() << kStageName << " must be run against the 'local' database",
                pExpCtx->ns.isLocalDB());
    }

    uassert(ErrorCodes::InvalidNamespace,
            str::stream() << kStageName
                          << " cannot be executed on an aggregation against a collection."
                          << " Run the aggregation against the database instead.",
            pExpCtx->ns.isCollectionlessAggregateNS());

    uassert(
        ErrorCodes::InvalidNamespace,
        str::stream() << kStageName
                      << " cannot be part of a query that references any collection or database.",
        pExpCtx->noForeignNamespaces());

    uassert(ErrorCodes::CannotBackup,
            str::stream() << kStageName << " cannot be executed against a router.",
            !pExpCtx->inRouter && !pExpCtx->fromRouter && !pExpCtx->needsMerge);

    // Parse $backupCursor arguments for incremental backups.
    BackupCursorParameters params = BackupCursorParameters::parse(IDLParserContext(""), spec.Obj());

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
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << "Cannot have 'thisBackupName' or 'srcBackupName' when "
                                 "incrementalBackup=false",
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

    return make_intrusive<DocumentSourceBackupCursor>(pExpCtx, std::move(params), options);
}

Value DocumentSourceBackupCursor::serialize(const SerializationOptions& opts) const {
    return Value(Document{{kStageName, _params.toBSON(opts)}});
}

}  // namespace mongo
