/**
 * Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "document_source_backup_cursor_extend.h"

#include <vector>

#include "mongo/bson/bsonmisc.h"
#include "mongo/db/s/replica_set_endpoint_feature_flag.h"

namespace mongo {

namespace {
const StringData kBackupIdFieldName = "backupId"_sd;
const StringData kTimestampFieldName = "timestamp"_sd;
}  // namespace

REGISTER_DOCUMENT_SOURCE(backupCursorExtend,
                         DocumentSourceBackupCursorExtend::LiteParsed::parse,
                         DocumentSourceBackupCursorExtend::createFromBson,
                         AllowedWithApiStrict::kAlways);

DocumentSourceBackupCursorExtend::DocumentSourceBackupCursorExtend(
    const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
    const UUID& backupId,
    const Timestamp& extendTo)
    : DocumentSource(kStageName, pExpCtx),
      _backupId(backupId),
      _extendTo(extendTo),
      _backupCursorExtendState(pExpCtx->getMongoProcessInterface()->extendBackupCursor(
          pExpCtx->getOperationContext(), backupId, extendTo)) {}

DocumentSource::GetNextResult DocumentSourceBackupCursorExtend::doGetNext() {
    if (!_backupCursorExtendState.filePaths.empty()) {
        Document doc = {{"filename", _backupCursorExtendState.filePaths.front()},
                        {"required", true}};
        _backupCursorExtendState.filePaths.pop_front();
        return {std::move(doc)};
    }

    return GetNextResult::makeEOF();
}

boost::intrusive_ptr<DocumentSource> DocumentSourceBackupCursorExtend::createFromBson(
    BSONElement spec, const boost::intrusive_ptr<ExpressionContext>& pExpCtx) {
    uassert(ErrorCodes::FailedToParse,
            str::stream() << kStageName
                          << " value must be an object. Found: " << typeName(spec.type()),
            spec.type() == BSONType::Object);

    if (replica_set_endpoint::isFeatureFlagEnabled()) {
        uassert(ErrorCodes::InvalidNamespace,
                str::stream() << kStageName << " must be run against the 'local' database",
                pExpCtx->getNamespaceString().isLocalDB());
    }

    uassert(ErrorCodes::InvalidNamespace,
            str::stream() << kStageName
                          << " cannot be executed on an aggregation against a collection."
                          << " Run the aggregation against the database instead.",
            pExpCtx->getNamespaceString().isCollectionlessAggregateNS());

    uassert(
        ErrorCodes::InvalidNamespace,
        str::stream() << kStageName
                      << " cannot be part of a query that references any collection or database.",
        pExpCtx->noForeignNamespaces());

    uassert(ErrorCodes::CannotBackup,
            str::stream() << kStageName << " cannot be executed against a router.",
            !pExpCtx->getInRouter() && !pExpCtx->getFromRouter() && !pExpCtx->getNeedsMerge());

    boost::optional<UUID> backupId = boost::none;
    boost::optional<Timestamp> extendTo = boost::none;

    for (auto&& elem : spec.embeddedObject()) {
        const auto fieldName = elem.fieldNameStringData();
        if (fieldName == kBackupIdFieldName) {
            uassert(ErrorCodes::FailedToParse,
                    str::stream() << "The '" << kBackupIdFieldName << "' parameter of the "
                                  << kStageName
                                  << " stage must be a BinData value, but found: " << elem.type(),
                    elem.type() == BSONType::BinData);
            uassert(ErrorCodes::FailedToParse,
                    str::stream() << "The '" << kBackupIdFieldName << "' parameter of the "
                                  << kStageName
                                  << " stage must be a BinData value of subtype UUID, but found: "
                                  << elem.binDataType(),
                    elem.binDataType() == BinDataType::newUUID);
            backupId = uassertStatusOK(UUID::parse(elem));
        } else if (fieldName == kTimestampFieldName) {
            uassert(ErrorCodes::FailedToParse,
                    str::stream() << "The '" << kTimestampFieldName << "' parameter of the "
                                  << kStageName << " stage must be a Timestamp value, but found: "
                                  << typeName(elem.type()),
                    elem.type() == BSONType::bsonTimestamp);
            extendTo = elem.timestamp();
            uassert(ErrorCodes::FailedToParse,
                    str::stream() << "The '" << kTimestampFieldName << "' parameter of the "
                                  << kStageName << " must not be Timestamp(0, 0).",
                    !extendTo->isNull());
        } else {
            uasserted(ErrorCodes::FailedToParse,
                      str::stream() << "Unrecognized option '" << fieldName
                                    << "' in $backupCursorExtend stage.");
        }
    }

    uassert(ErrorCodes::FailedToParse,
            str::stream() << "'" << kBackupIdFieldName << "' parameter is missing",
            backupId);
    uassert(ErrorCodes::FailedToParse,
            str::stream() << "'" << kTimestampFieldName << "' parameter is missing",
            extendTo);

    return new DocumentSourceBackupCursorExtend(pExpCtx, *backupId, *extendTo);
}

Value DocumentSourceBackupCursorExtend::serialize(const SerializationOptions& opts) const {
    return Value(Document{{kStageName,
                           Document{{kBackupIdFieldName, opts.serializeLiteral(_backupId)},
                                    {kTimestampFieldName, opts.serializeLiteral(_extendTo)}}}});
}
}  // namespace mongo
