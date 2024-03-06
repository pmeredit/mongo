/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "document_source_backup_cursor_extend.h"

#include <vector>

#include "mongo/bson/bsonmisc.h"

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
      _backupCursorExtendState(
          pExpCtx->mongoProcessInterface->extendBackupCursor(pExpCtx->opCtx, backupId, extendTo)) {}

DocumentSource::GetNextResult DocumentSourceBackupCursorExtend::doGetNext() {
    if (!_backupCursorExtendState.filenames.empty()) {
        Document doc = {{"filename", _backupCursorExtendState.filenames.front()},
                        {"required", true}};
        _backupCursorExtendState.filenames.pop_front();
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

    uassert(ErrorCodes::CannotBackup,
            str::stream() << kStageName << " cannot be executed against a MongoS.",
            !pExpCtx->inMongos && !pExpCtx->fromMongos && !pExpCtx->needsMerge);

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
    return Value(BSON(kStageName << BSON(kBackupIdFieldName << opts.serializeLiteral(_backupId)
                                                            << kTimestampFieldName
                                                            << opts.serializeLiteral(_extendTo))));
}
}  // namespace mongo
