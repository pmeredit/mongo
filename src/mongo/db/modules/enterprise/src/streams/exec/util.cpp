/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/util.h"

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/db/pipeline/name_expression.h"
#include "streams/exec/constants.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/mongodb_process_interface.h"
#include "streams/util/exception.h"

using namespace mongo;

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

bool isSourceStage(mongo::StringData name) {
    return name == mongo::StringData(kSourceStageName);
}

bool isSinkStage(mongo::StringData name) {
    return isEmitStage(name) || isMergeStage(name);
}

bool isWindowStage(mongo::StringData name) {
    return name == kTumblingWindowStageName || name == kHoppingWindowStageName;
}

bool isLookUpStage(mongo::StringData name) {
    return name == mongo::StringData(kLookUpStageName);
}

bool isEmitStage(mongo::StringData name) {
    return name == mongo::StringData(kEmitStageName);
}

bool isMergeStage(mongo::StringData name) {
    return name == mongo::StringData(kMergeStageName);
}

bool isWindowAwareStage(mongo::StringData name) {
    static const stdx::unordered_set<StringData> windowAwareStages{
        {kGroupStageName, kSortStageName, kLimitStageName}};
    return windowAwareStages.contains(name);
}

// TODO(STREAMS-220)-PrivatePreview: Especially with units of day and year,
// this logic probably leads to incorrect for daylights savings time and leap years.
// In future work we can rely more on std::chrono for the time math here, for now
// we're just converting the size and slide to milliseconds for simplicity.
int64_t toMillis(mongo::StreamTimeUnitEnum unit, int count) {
    switch (unit) {
        case StreamTimeUnitEnum::Millisecond:
            return stdx::chrono::milliseconds(count).count();
        case StreamTimeUnitEnum::Second:
            return stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                       stdx::chrono::seconds(count))
                .count();
        case StreamTimeUnitEnum::Minute:
            return stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                       stdx::chrono::minutes(count))
                .count();
        case StreamTimeUnitEnum::Hour:
            return stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                       stdx::chrono::hours(count))
                .count();
        // For Day, using the C++20 ratios from
        // https://en.cppreference.com/w/cpp/header/chrono
        case StreamTimeUnitEnum::Day:
            return stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                       stdx::chrono::seconds(86400 * count))
                .count();
        default:
            MONGO_UNREACHABLE;
    }
}

BSONObjBuilder toDeadLetterQueueMsg(const boost::optional<std::string>& streamMetaFieldName,
                                    StreamMeta streamMeta,
                                    boost::optional<std::string> error) {
    BSONObjBuilder objBuilder;
    if (streamMetaFieldName) {
        objBuilder.append(*streamMetaFieldName, streamMeta.toBSON());
    }
    if (error) {
        objBuilder.append("errInfo", BSON("reason" << *error));
    }
    return objBuilder;
}

BSONObjBuilder toDeadLetterQueueMsg(const boost::optional<std::string>& streamMetaFieldName,
                                    StreamDocument streamDoc,
                                    boost::optional<std::string> error) {
    BSONObjBuilder objBuilder = toDeadLetterQueueMsg(
        streamMetaFieldName, std::move(streamDoc.streamMeta), std::move(error));
    objBuilder.append("fullDocument", streamDoc.doc.toBson());
    return objBuilder;
}

NamespaceString getNamespaceString(const std::string& dbStr, const std::string& collStr) {
    return NamespaceStringUtil::deserialize(
        DatabaseNameUtil::deserialize(/*tenantId=*/boost::none, dbStr, SerializationContext()),
        collStr);
}

NamespaceString getNamespaceString(const NameExpression& db, const NameExpression& coll) {
    using namespace fmt::literals;
    tassert(8117400,
            "Expected a static database name but got expression: {}"_format(db.toString()),
            db.isLiteral());
    auto dbStr = db.getLiteral();
    tassert(8117401,
            "Expected a static collection name but got expression: {}"_format(coll.toString()),
            coll.isLiteral());
    auto collStr = coll.getLiteral();
    return getNamespaceString(dbStr, collStr);
}

write_ops::WriteError getWriteErrorIndexFromRawServerError(
    const bsoncxx::document::value& rawServerError) {
    using namespace mongo::write_ops;
    using namespace fmt::literals;

    // Here is the expected schema of 'rawServerError':
    // https://github.com/mongodb/specifications/blob/master/source/driver-bulk-update.rst#merging-write-errors
    auto rawServerErrorObj = fromBsoncxxDocument(rawServerError);

    // Extract write error indexes.
    auto writeErrorsVec = rawServerErrorObj[kWriteErrorsFieldName].Array();
    constexpr auto writeErrorLess = [](const mongo::write_ops::WriteError& lhs,
                                       const mongo::write_ops::WriteError& rhs) {
        return lhs.getIndex() < rhs.getIndex();
    };
    std::set<WriteError, decltype(writeErrorLess)> writeErrors;
    for (auto& writeErrorElem : writeErrorsVec) {
        writeErrors.insert(WriteError::parse(writeErrorElem.embeddedObject()));
    }
    uassert(ErrorCodes::InternalError,
            "bulk_write_exception::raw_server_error() contains duplicate entries in the "
            "'{}' field"_format(kWriteErrorsFieldName),
            writeErrors.size() == writeErrorsVec.size());

    // Since we apply the writes in ordered manner there should only be 1 failed write and all the
    // writes before it should have succeeded.
    uassert(ErrorCodes::InternalError,
            str::stream() << "bulk_write_exception::raw_server_error() contains unexpected ("
                          << writeErrors.size() << ") number of write error",
            writeErrors.size() == 1);

    // Extract upserted indexes.
    auto upserted = rawServerErrorObj[UpdateCommandReply::kUpsertedFieldName];
    std::set<size_t> upsertedIndexes;
    if (!upserted.eoo()) {
        auto upsertedVec = upserted.Array();
        for (auto& upsertedItem : upsertedVec) {
            upsertedIndexes.insert(upsertedItem[Upserted::kIndexFieldName].Int());
        }
        uassert(ErrorCodes::InternalError,
                "bulk_write_exception::raw_server_error() contains duplicate entries in the "
                "'{}' field"_format(UpdateCommandReply::kUpsertedFieldName),
                upsertedIndexes.size() == upsertedVec.size());
        uassert(ErrorCodes::InternalError,
                str::stream() << "unexpected number of upserted indexes (" << upsertedIndexes.size()
                              << " vs " << writeErrors.size() << ")",
                upsertedIndexes.size() == size_t(writeErrors.begin()->getIndex()));
        size_t i = 0;
        for (auto idx : upsertedIndexes) {
            uassert(ErrorCodes::InternalError,
                    str::stream() << "unexpected upserted index value (" << idx << " vs " << i
                                  << ")",
                    idx == i);
            ++i;
        }
    }
    return *writeErrors.begin();
}

}  // namespace streams
