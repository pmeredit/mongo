/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
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
    return name == kTumblingWindowStageName || name == kHoppingWindowStageName ||
        name == kSessionWindowStageName;
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

bool isBlockingWindowAwareStage(mongo::StringData name) {
    static const stdx::unordered_set<StringData> blockingWindowAwareStages{
        {kGroupStageName, kSortStageName}};
    return blockingWindowAwareStages.contains(name);
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
                                    const StreamMeta& streamMeta,
                                    const std::string& operatorName,
                                    boost::optional<std::string> error) {
    BSONObjBuilder objBuilder;
    if (streamMetaFieldName) {
        objBuilder.append(*streamMetaFieldName, streamMeta.toBSON());
    }
    if (error) {
        objBuilder.append("errInfo", BSON("reason" << *error));
    }
    objBuilder.append("operatorName", operatorName);

    return objBuilder;
}

BSONObjBuilder toDeadLetterQueueMsg(const boost::optional<std::string>& streamMetaFieldName,
                                    const StreamDocument& streamDoc,
                                    const std::string& operatorName,
                                    boost::optional<std::string> error) {
    BSONObjBuilder objBuilder = toDeadLetterQueueMsg(
        streamMetaFieldName, streamDoc.streamMeta, operatorName, std::move(error));
    const int64_t maxDlqFullDocumentSizeBytes = BSONObjMaxUserSize / 2;
    if (streamDoc.doc.getApproximateSize() <= maxDlqFullDocumentSizeBytes) {
        objBuilder.append("doc", streamDoc.doc.toBson());
    } else {
        objBuilder.append("doc", "full document too large to emit");
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
    if (internalStreamMeta.getSource()) {
        if (internalStreamMeta.getSource()->getType()) {
            newStreamMeta.setNestedField(
                FieldPath((str::stream() << StreamMeta::kSourceFieldName << "."
                                         << StreamMetaSource::kTypeFieldName)
                              .ss.str()),
                Value(StreamMetaSourceType_serializer(*internalStreamMeta.getSource()->getType())));
        }
        if (internalStreamMeta.getSource()->getTopic()) {
            newStreamMeta.setNestedField(
                FieldPath((str::stream() << StreamMeta::kSourceFieldName << "."
                                         << StreamMetaSource::kTopicFieldName)
                              .ss.str()),
                Value(*internalStreamMeta.getSource()->getTopic()));
        }
        if (internalStreamMeta.getSource()->getPartition()) {
            newStreamMeta.setNestedField(
                FieldPath((str::stream() << StreamMeta::kSourceFieldName << "."
                                         << StreamMetaSource::kPartitionFieldName)
                              .ss.str()),
                Value(*internalStreamMeta.getSource()->getPartition()));
        }
        if (internalStreamMeta.getSource()->getOffset()) {
            newStreamMeta.setNestedField(
                FieldPath((str::stream() << StreamMeta::kSourceFieldName << "."
                                         << StreamMetaSource::kOffsetFieldName)
                              .ss.str()),
                Value(static_cast<long long>(*internalStreamMeta.getSource()->getOffset())));
        }
        if (internalStreamMeta.getSource()->getKey()) {
            auto keyPath = FieldPath((str::stream() << StreamMeta::kSourceFieldName << "."
                                                    << StreamMetaSource::kKeyFieldName)
                                         .ss.str());
            std::visit(
                OverloadedVisitor{
                    [&](const std::vector<uint8_t>& key) {
                        if (key.size()) {
                            BSONBinData binData(key.data(), key.size(), mongo::BinDataGeneral);
                            newStreamMeta.setNestedField(keyPath, Value(std::move(binData)));
                        }
                    },
                    [&](const std::string& key) {
                        newStreamMeta.setNestedField(keyPath, Value(key));
                    },
                    [&](const mongo::BSONObj& key) {
                        newStreamMeta.setNestedField(keyPath, Value(key));
                    },
                    [&](int32_t key) { newStreamMeta.setNestedField(keyPath, Value(key)); },
                    [&](int64_t key) {
                        newStreamMeta.setNestedField(keyPath, Value(static_cast<long long>(key)));
                    },
                    [&](double key) { newStreamMeta.setNestedField(keyPath, Value(key)); }},
                *internalStreamMeta.getSource()->getKey());
        }
        if (internalStreamMeta.getSource()->getHeaders()) {
            if (!internalStreamMeta.getSource()->getHeaders()->empty()) {
                std::vector<mongo::Value> headers;
                headers.reserve(internalStreamMeta.getSource()->getHeaders()->size());
                for (const auto& header : *internalStreamMeta.getSource()->getHeaders()) {
                    headers.emplace_back(Value(header.toBSON()));
                }
                newStreamMeta.setNestedField(
                    FieldPath((str::stream() << StreamMeta::kSourceFieldName << "."
                                             << StreamMetaSource::kHeadersFieldName)
                                  .ss.str()),
                    Value(std::move(headers)));
            }
        }
    }
    if (internalStreamMeta.getWindow()) {
        if (internalStreamMeta.getWindow()->getStart()) {
            newStreamMeta.setNestedField(
                FieldPath((str::stream() << StreamMeta::kWindowFieldName << "."
                                         << StreamMetaWindow::kStartFieldName)
                              .ss.str()),
                Value(*internalStreamMeta.getWindow()->getStart()));
        }
        if (internalStreamMeta.getWindow()->getEnd()) {
            newStreamMeta.setNestedField(
                FieldPath((str::stream() << StreamMeta::kWindowFieldName << "."
                                         << StreamMetaWindow::kEndFieldName)
                              .ss.str()),
                Value(*internalStreamMeta.getWindow()->getEnd()));
        }
        if (internalStreamMeta.getWindow()->getPartition()) {
            newStreamMeta.setNestedField(
                FieldPath((str::stream() << StreamMeta::kWindowFieldName << "."
                                         << StreamMetaWindow::kPartitionFieldName)
                              .ss.str()),
                *internalStreamMeta.getWindow()->getPartition());
        }
    }

    if (auto externalAPI = internalStreamMeta.getExternalAPI(); externalAPI) {
        newStreamMeta.setNestedField(
            FieldPath((str::stream() << StreamMeta::kExternalAPIFieldName << "."
                                     << StreamMetaExternalAPI::kUrlFieldName)
                          .ss.str()),
            Value(externalAPI->getUrl()));

        newStreamMeta.setNestedField(
            FieldPath((str::stream() << StreamMeta::kExternalAPIFieldName << "."
                                     << StreamMetaExternalAPI::kRequestTypeFieldName)
                          .ss.str()),
            Value(HttpMethod_serializer(externalAPI->getRequestType())));

        newStreamMeta.setNestedField(
            FieldPath((str::stream() << StreamMeta::kExternalAPIFieldName << "."
                                     << StreamMetaExternalAPI::kHttpStatusCodeFieldName)
                          .ss.str()),
            Value(externalAPI->getHttpStatusCode()));

        newStreamMeta.setNestedField(
            FieldPath((str::stream() << StreamMeta::kExternalAPIFieldName << "."
                                     << StreamMetaExternalAPI::kResponseTimeMsFieldName)
                          .ss.str()),
            Value(externalAPI->getResponseTimeMs()));
    }
    return newStreamMeta.freeze();
}

constexpr const char kConfluentCloud[] = "confluent.cloud";

bool isConfluentBroker(const std::string& bootstrapServers) {
    return mongo::str::contains(bootstrapServers, kConfluentCloud);
}

}  // namespace streams
