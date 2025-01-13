/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/util.h"

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/name_expression.h"
#include "mongo/util/time_support.h"
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

bool hasWindow(const std::vector<BSONObj>& pipeline) {
    for (const auto& stage : pipeline) {
        if (isWindowStage(stage.firstElementFieldNameStringData())) {
            return true;
        }
    }
    return false;
}

bool hasHttpsStage(const std::vector<mongo::BSONObj>& pipeline) {
    for (const auto& stage : pipeline) {
        if (isHttpsStage(stage.firstElementFieldNameStringData())) {
            return true;
        }
    }
    return false;
}

bool hasHttpsStageBeforeWindow(const std::vector<mongo::BSONObj>& pipeline) {
    boost::optional<size_t> httpsStageIdx;
    boost::optional<size_t> windowStageIdx;
    for (size_t idx = 0; idx < pipeline.size(); ++idx) {
        const auto& stage = pipeline[idx];
        auto name = stage.firstElementFieldNameStringData();
        if (isWindowStage(name)) {
            windowStageIdx = idx;
        } else if (isHttpsStage(name)) {
            httpsStageIdx = idx;
        }
    }
    if (!httpsStageIdx || !windowStageIdx) {
        return false;
    }
    return *httpsStageIdx < *windowStageIdx;
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

bool hasBlockingStage(const BSONPipeline& pipeline) {
    for (const auto& stage : pipeline) {
        if (isBlockingWindowAwareStage(stage.firstElementFieldNameStringData())) {
            return true;
        }
    }
    return false;
}

bool isHttpsStage(mongo::StringData name) {
    return name == mongo::StringData(kHttpsStageName);
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

BSONObjBuilder toDeadLetterQueueMsg(const boost::optional<std::string>& streamMetaFieldName,
                                    const StreamMeta& streamMeta,
                                    const Document& doc,
                                    const std::string& operatorName,
                                    boost::optional<std::string> error) {
    BSONObjBuilder objBuilder =
        toDeadLetterQueueMsg(streamMetaFieldName, streamMeta, operatorName, std::move(error));
    const int64_t maxDlqFullDocumentSizeBytes = BSONObjMaxUserSize / 2;
    if (doc.getApproximateSize() <= maxDlqFullDocumentSizeBytes) {
        objBuilder.append("doc", doc.toBson());
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

    if (auto httpsMeta = internalStreamMeta.getHttps(); httpsMeta) {
        newStreamMeta.setNestedField(FieldPath((str::stream() << StreamMeta::kHttpsFieldName << "."
                                                              << StreamMetaHttps::kUrlFieldName)
                                                   .ss.str()),
                                     Value(httpsMeta->getUrl()));

        newStreamMeta.setNestedField(FieldPath((str::stream() << StreamMeta::kHttpsFieldName << "."
                                                              << StreamMetaHttps::kMethodFieldName)
                                                   .ss.str()),
                                     Value(HttpMethod_serializer(httpsMeta->getMethod())));

        newStreamMeta.setNestedField(
            FieldPath((str::stream() << StreamMeta::kHttpsFieldName << "."
                                     << StreamMetaHttps::kHttpStatusCodeFieldName)
                          .ss.str()),
            Value(httpsMeta->getHttpStatusCode()));

        newStreamMeta.setNestedField(
            FieldPath((str::stream() << StreamMeta::kHttpsFieldName << "."
                                     << StreamMetaHttps::kResponseTimeMsFieldName)
                          .ss.str()),
            Value(httpsMeta->getResponseTimeMs()));
    }
    return newStreamMeta.freeze();
}

constexpr const char kConfluentCloud[] = "confluent.cloud";

bool isConfluentBroker(const std::string& bootstrapServers) {
    return mongo::str::contains(bootstrapServers, kConfluentCloud);
}

std::vector<StringData> getLoggablePipeline(const std::vector<BSONObj>& pipeline) {
    std::vector<StringData> stageNames;
    stageNames.reserve(pipeline.size());

    for (const BSONObj& stage : pipeline) {
        stageNames.push_back(stage.firstElementFieldNameStringData());
    }

    return stageNames;
}

std::string convertDateToISO8601(mongo::Date_t date) {
    return dateToISOStringUTC(date);
}

std::vector<mongo::Value> convertDateToISO8601(const std::vector<mongo::Value>& arr) {
    std::vector<mongo::Value> result;
    result.reserve(arr.size());
    for (const auto& value : arr) {
        auto type = value.getType();
        if (type == BSONType::Date) {
            result.push_back(Value(convertDateToISO8601(value.getDate())));
        } else if (type == BSONType::Object) {
            result.push_back(Value(convertDateToISO8601(value.getDocument())));
        } else if (type == BSONType::Array) {
            result.push_back(Value(convertDateToISO8601(value.getArray())));
        } else {
            result.push_back(value);
        }
    }
    return result;
}

mongo::Document convertDateToISO8601(mongo::Document doc) {
    MutableDocument mut(doc);
    auto it = doc.fieldIterator();
    while (it.more()) {
        const auto& [fieldName, field] = it.next();
        auto type = doc[fieldName].getType();
        if (type == BSONType::Date) {
            mut.setField(fieldName, Value(convertDateToISO8601(field.getDate())));
        } else if (type == BSONType::Object) {
            mut.setField(fieldName, Value(convertDateToISO8601(field.getDocument())));
        } else if (type == BSONType::Array) {
            mut.setField(fieldName, Value(convertDateToISO8601(field.getArray())));
        }
    }
    return mut.freeze();
}

}  // namespace streams
