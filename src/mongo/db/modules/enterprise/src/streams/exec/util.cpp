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
    const int64_t maxDlqFullDocumentSizeBytes = BSONObjMaxUserSize / 2;
    if (streamDoc.doc.getApproximateSize() <= maxDlqFullDocumentSizeBytes) {
        objBuilder.append("fullDocument", streamDoc.doc.toBson());
    } else {
        objBuilder.append("fullDocument", "fullDocument too large to emit");
    }
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

mongo::Document updateStreamMeta(const mongo::Value& streamMetaInDoc,
                                 const mongo::StreamMeta& internalStreamMeta) {
    MutableDocument newStreamMeta(streamMetaInDoc.isObject() ? streamMetaInDoc.getDocument()
                                                             : Document());
    if (internalStreamMeta.getTimestamp()) {
        newStreamMeta.setField(mongo::StreamMeta::kTimestampFieldName,
                               Value(*internalStreamMeta.getTimestamp()));
    }
    if (internalStreamMeta.getSourceType()) {
        newStreamMeta.setField(
            mongo::StreamMeta::kSourceTypeFieldName,
            Value(StreamMetaSourceType_serializer(*internalStreamMeta.getSourceType())));
    }
    if (internalStreamMeta.getSourcePartition()) {
        newStreamMeta.setField(mongo::StreamMeta::kSourcePartitionFieldName,
                               Value(*internalStreamMeta.getSourcePartition()));
    }
    if (internalStreamMeta.getSourceOffset()) {
        newStreamMeta.setField(
            mongo::StreamMeta::kSourceOffsetFieldName,
            Value(static_cast<long long>(*internalStreamMeta.getSourceOffset())));
    }
    if (internalStreamMeta.getSourceKey()) {
        if (internalStreamMeta.getSourceKey()->length()) {
            BSONBinData binData(internalStreamMeta.getSourceKey()->data(),
                                internalStreamMeta.getSourceKey()->length(),
                                mongo::BinDataGeneral);
            newStreamMeta.setField(mongo::StreamMeta::kSourceKeyFieldName,
                                   Value(std::move(binData)));
        }
    }
    if (internalStreamMeta.getSourceHeaders()) {
        if (!internalStreamMeta.getSourceHeaders()->empty()) {
            std::vector<mongo::Value> headers;
            headers.reserve(internalStreamMeta.getSourceHeaders()->size());
            for (const auto& header : *internalStreamMeta.getSourceHeaders()) {
                headers.emplace_back(Value(header.toBSON()));
            }
            newStreamMeta.setField(mongo::StreamMeta::kSourceHeadersFieldName,
                                   Value(std::move(headers)));
        }
    }
    if (internalStreamMeta.getWindowStart()) {
        newStreamMeta.setField(mongo::StreamMeta::kWindowStartFieldName,
                               Value(*internalStreamMeta.getWindowStart()));
    }
    if (internalStreamMeta.getWindowEnd()) {
        newStreamMeta.setField(mongo::StreamMeta::kWindowEndFieldName,
                               Value(*internalStreamMeta.getWindowEnd()));
    }
    return newStreamMeta.freeze();
}

}  // namespace streams
