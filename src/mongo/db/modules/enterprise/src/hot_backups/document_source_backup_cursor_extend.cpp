/**
 * Copyright (C) 2018 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
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
                         DocumentSourceBackupCursorExtend::createFromBson);

const char* DocumentSourceBackupCursorExtend::kStageName = "$backupCursorExtend";

DocumentSourceBackupCursorExtend::DocumentSourceBackupCursorExtend(
    const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
    const UUID& backupId,
    const Timestamp& extendTo)
    : DocumentSource(pExpCtx),
      _backupId(backupId),
      _extendTo(extendTo),
      _backupCursorExtendState(
          pExpCtx->mongoProcessInterface->extendBackupCursor(pExpCtx->opCtx, backupId, extendTo)) {}

DocumentSource::GetNextResult DocumentSourceBackupCursorExtend::getNext() {
    pExpCtx->checkForInterrupt();

    if (!_backupCursorExtendState.filenames.empty()) {
        Document doc = {{"filename", _backupCursorExtendState.filenames.back()}};
        _backupCursorExtendState.filenames.pop_back();
        return {std::move(doc)};
    }

    return GetNextResult::makeEOF();
}

boost::intrusive_ptr<DocumentSource> DocumentSourceBackupCursorExtend::createFromBson(
    BSONElement spec, const boost::intrusive_ptr<ExpressionContext>& pExpCtx) {
    uassert(
        ErrorCodes::FailedToParse,
        str::stream() << kStageName << " value must be an object. Found: " << typeName(spec.type()),
        spec.type() == BSONType::Object);

    uassert(ErrorCodes::CannotBackup,
            str::stream() << kStageName << " cannot be executed against a MongoS.",
            !pExpCtx->inMongos && !pExpCtx->fromMongos && !pExpCtx->needsMerge);

    boost::optional<UUID> backupId = boost::none;
    boost::optional<Timestamp> extendTo = boost::none;

    for (auto&& elem : spec.embeddedObject()) {
        const auto fieldName = elem.fieldNameStringData();
        if (fieldName == kBackupIdFieldName) {
            uassert(ErrorCodes::FailedToParse,
                    str::stream() << "The '" << kBackupIdFieldName << "' parameter of the "
                                  << kStageName
                                  << " stage must be a BinData value, but found: "
                                  << elem.type(),
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
                                  << kStageName
                                  << " stage must be a Timestamp value, but found: "
                                  << typeName(elem.type()),
                    elem.type() == BSONType::bsonTimestamp);
            extendTo = elem.timestamp();
            uassert(ErrorCodes::FailedToParse,
                    str::stream() << "The '" << kTimestampFieldName << "' parameter of the "
                                  << kStageName
                                  << " must not be Timestamp(0, 0).",
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

Value DocumentSourceBackupCursorExtend::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(BSON(
        kStageName << BSON(kBackupIdFieldName << _backupId << kTimestampFieldName << _extendTo)));
}
}  // namespace mongo
