/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/util.h"

#include <bsoncxx/exception/exception.hpp>
#include <iostream>
#include <limits>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/bsontypes_util.h"
#include "mongo/bson/json.h"
#include "mongo/bson/oid.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/pipeline/name_expression.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/hex.h"
#include "mongo/util/time_support.h"
#include "streams/exec/constants.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/mongodb_process_interface.h"

using namespace mongo;

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

namespace {
constexpr StringData kPositiveInfinityValue = "Infinity"_sd;
constexpr StringData kNegativeInfinityValue = "-Infinity"_sd;
}  // namespace

bool isSourceStage(mongo::StringData name) {
    return name == mongo::StringData(kSourceStageName);
}

bool isSinkStage(mongo::StringData name) {
    return isSinkOnlyStage(name) || isMiddleAndSinkStage(name);
}

bool isSinkOnlyStage(mongo::StringData name) {
    return isEmitStage(name) || isMergeStage(name);
}

bool isMiddleAndSinkStage(mongo::StringData name) {
    return isExternalFunctionStage(name);
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

bool hasPayloadStage(const std::vector<mongo::BSONObj>& pipeline) {
    for (const auto& stage : pipeline) {
        if (isPayloadStage(stage.firstElementFieldNameStringData())) {
            return true;
        }
    }
    return false;
}

bool hasPayloadStageBeforeWindow(const std::vector<mongo::BSONObj>& pipeline) {
    boost::optional<size_t> payloadStageIdx;
    boost::optional<size_t> windowStageIdx;
    for (size_t idx = 0; idx < pipeline.size(); ++idx) {
        const auto& stage = pipeline[idx];
        auto name = stage.firstElementFieldNameStringData();
        if (isWindowStage(name)) {
            windowStageIdx = idx;
        } else if (isPayloadStage(name)) {
            payloadStageIdx = idx;
        }
    }
    if (!payloadStageIdx || !windowStageIdx) {
        return false;
    }
    return *payloadStageIdx < *windowStageIdx;
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

bool isLimitStage(mongo::StringData name) {
    return name == mongo::StringData(kLimitStageName);
}

bool isExternalFunctionStage(mongo::StringData name) {
    return name == mongo::StringData(kExternalFunctionStageName);
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

bool isPayloadStage(mongo::StringData name) {
    return name == mongo::StringData(kHttpsStageName) || isExternalFunctionStage(name);
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

mongo::BSONObjBuilder toDeadLetterQueueMsg(const boost::optional<std::string>& streamMetaFieldName,
                                           const StreamDocument& streamDoc,
                                           const std::string& operatorName,
                                           mongo::Status status) {
    return toDeadLetterQueueMsg(streamMetaFieldName, streamDoc, operatorName, status.reason());
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
    tassert(8117400,
            fmt::format("Expected a static database name but got expression: {}", db.toString()),
            db.isLiteral());
    auto dbStr = db.getLiteral();
    tassert(
        8117401,
        fmt::format("Expected a static collection name but got expression: {}", coll.toString()),
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
            newStreamMeta.setNestedField(keyPath, *internalStreamMeta.getSource()->getKey());
        }
        if (internalStreamMeta.getSource()->getHeaders()) {
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
    if (auto externalFunctionMeta = internalStreamMeta.getExternalFunction();
        externalFunctionMeta) {
        newStreamMeta.setNestedField(
            FieldPath((str::stream() << StreamMeta::kExternalFunctionFieldName << "."
                                     << StreamMetaExternalFunction::kFunctionNameFieldName)
                          .ss.str()),
            Value(externalFunctionMeta->getFunctionName()));

        newStreamMeta.setNestedField(
            FieldPath((str::stream() << StreamMeta::kExternalFunctionFieldName << "."
                                     << StreamMetaExternalFunction::kExecutedVersionFieldName)
                          .ss.str()),
            Value(externalFunctionMeta->getExecutedVersion()));

        newStreamMeta.setNestedField(
            FieldPath((str::stream() << StreamMeta::kExternalFunctionFieldName << "."
                                     << StreamMetaExternalFunction::kStatusCodeFieldName)
                          .ss.str()),
            Value(externalFunctionMeta->getStatusCode()));

        if (externalFunctionMeta->getFunctionError()) {
            newStreamMeta.setNestedField(
                FieldPath((str::stream() << StreamMeta::kExternalFunctionFieldName << "."
                                         << StreamMetaExternalFunction::kFunctionErrorFieldName)
                              .ss.str()),
                Value(*externalFunctionMeta->getFunctionError()));
        }

        newStreamMeta.setNestedField(
            FieldPath((str::stream() << StreamMeta::kExternalFunctionFieldName << "."
                                     << StreamMetaExternalFunction::kResponseTimeMsFieldName)
                          .ss.str()),
            Value(externalFunctionMeta->getResponseTimeMs()));
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

mongo::Document convertAllFields(
    mongo::Document doc, const std::function<mongo::Value(const mongo::Value&)> convertFunc) {
    MutableDocument mut(doc);
    auto it = doc.fieldIterator();
    while (it.more()) {
        const auto& [fieldName, field] = it.next();
        auto type = doc[fieldName].getType();
        if (type == BSONType::Object) {
            mut.setField(fieldName, Value(convertAllFields(field.getDocument(), convertFunc)));
        } else if (type == BSONType::Array) {
            mut.setField(fieldName, Value(convertAllFields(field.getArray(), convertFunc)));
        } else {
            mut.setField(fieldName, convertFunc(field));
        }
    }
    return mut.freeze();
}

std::vector<mongo::Value> convertAllFields(
    const std::vector<mongo::Value>& arr,
    const std::function<mongo::Value(const mongo::Value&)> convertFunc) {
    std::vector<mongo::Value> result;
    result.reserve(arr.size());
    for (const auto& value : arr) {
        auto type = value.getType();
        if (type == BSONType::Object) {
            result.push_back(Value(convertAllFields(value.getDocument(), convertFunc)));
        } else if (type == BSONType::Array) {
            result.push_back(Value(convertAllFields(value.getArray(), convertFunc)));
        } else {
            result.push_back(convertFunc(value));
        }
    }
    return result;
}

mongo::Value dateToISO8601(const mongo::Value& value) {
    if (value.getType() != BSONType::Date) {
        return value;
    }
    auto result = TimeZoneDatabase::utcZone().formatDate(kIsoFormatStringZ, value.getDate());
    uassertStatusOK(result);
    return Value(result.getValue());
}

mongo::Document convertDateToISO8601(mongo::Document doc) {
    return convertAllFields(std::move(doc), dateToISO8601);
}

bool startsWithOpeningBrace(StringData s) {
    for (char c : s) {
        if (!std::isspace(c)) {
            return c == '{';
        }
    }
    return false;
}

bool startsWithOpeningBracket(StringData s) {
    for (char c : s) {
        if (!std::isspace(c)) {
            return c == '[';
        }
    }
    return false;
}

mongo::Value jsonStringsToJson(const mongo::Value& value) {
    if (value.getType() != BSONType::String) {
        return value;
    }
    auto jsonString = value.getStringData();

    if (startsWithOpeningBracket(jsonString)) {
        try {
            std::string objectWrapper = fmt::format(R"({{"data":{}}})", jsonString);
            auto responseView =
                bsoncxx::stdx::string_view{objectWrapper.data(), objectWrapper.size()};
            auto responseAsBson = fromBsoncxxDocument(bsoncxx::from_json(responseView));
            return Value(responseAsBson.firstElement());
        } catch (const bsoncxx::exception&) {
            // If we fail to convert the string to json, just return the original value
            return value;
        }
    }

    if (startsWithOpeningBrace(jsonString)) {
        try {
            auto responseView = bsoncxx::stdx::string_view{jsonString.data(), jsonString.size()};
            auto responseAsBson = fromBsoncxxDocument(bsoncxx::from_json(responseView));
            return Value(std::move(responseAsBson));
        } catch (const bsoncxx::exception&) {
            // If we fail to convert the string to json, just return the original value
            return value;
        }
    }

    return value;
}

mongo::Document convertJsonStringsToJson(mongo::Document doc) {
    return convertAllFields(std::move(doc), jsonStringsToJson);
}

std::vector<mongo::Value> jsonStringToValue(const std::vector<mongo::Value>& arr) {
    return convertAllFields(arr, jsonStringsToJson);
}

mongo::Value modifyValueForBasicJson(const mongo::Value& value) {
    switch (value.getType()) {
        case BSONType::jstOID:
            return mongo::Value{value.getOid().toString()};
        case BSONType::Date:
            return mongo::Value{value.getDate().toMillisSinceEpoch()};
        case BSONType::bsonTimestamp: {
            auto timestamp = duration_cast<Milliseconds>(Seconds{value.getTimestamp().getSecs()});
            return mongo::Value{static_cast<long long>(timestamp.count())};
        }
        case BSONType::NumberDecimal: {
            return mongo::Value{value.getDecimal().toString()};
        }
        case BSONType::BinData: {
            const auto& binValue = value.getBinData();
            switch (value.getBinData().type) {
                case BinDataType::newUUID:
                    return mongo::Value{value.getUuid().toString()};
                default:
                    return mongo::Value{base64::encode(
                        StringData(static_cast<const char*>(binValue.data), binValue.length))};
            }
            break;
        }
        case BSONType::NumberDouble: {
            auto doubleValue = value.getDouble();
            if (doubleValue == std::numeric_limits<double>::infinity()) {
                return mongo::Value{kPositiveInfinityValue};
            } else if (doubleValue == -std::numeric_limits<double>::infinity()) {
                return mongo::Value{kNegativeInfinityValue};
            }
            break;
        }
        case BSONType::RegEx:
            return mongo::Value{
                BSON("pattern" << value.getRegex() << "options" << value.getRegexFlags())};
        default:
            break;
    }
    return value;
}

mongo::Document modifyDocumentForBasicJson(const mongo::Document& doc) {
    return convertAllFields(doc, modifyValueForBasicJson);
}

std::string serializeJson(const BSONObj& bsonObj, JsonStringFormat format, bool pretty) {
    switch (format) {
        case JsonStringFormat::Canonical:
            return tojson(bsonObj, mongo::ExtendedCanonicalV2_0_0, pretty);
        case JsonStringFormat::Relaxed:
            return tojson(bsonObj, mongo::ExtendedRelaxedV2_0_0, pretty);
        case JsonStringFormat::Basic: {
            auto modifiedDoc = modifyDocumentForBasicJson(mongo::Document{bsonObj});
            return tojson(modifiedDoc.toBson(), mongo::ExtendedRelaxedV2_0_0, pretty);
        }
        default:
            tasserted(9997603, "Received unsupported json string format");
    }
}

mongo::Value parseAndDeserializeJsonResponse(StringData rawResponse, bool parseJsonStrings) {

    // TODO(SERVER-98467): parse the json array directly instead of wrapping in a doc
    if (rawResponse.front() == '[') {
        std::string objectWrapper = fmt::format(R"({{"data":{}}})", rawResponse);
        auto responseView = bsoncxx::stdx::string_view{objectWrapper.data(), objectWrapper.size()};
        auto responseAsBson = fromBsoncxxDocument(bsoncxx::from_json(responseView));
        auto jsonResponse = Value(responseAsBson.firstElement());
        if (!parseJsonStrings) {
            return jsonResponse;
        }
        auto responseArray = jsonStringToValue(jsonResponse.getArray());
        return Value(std::move(responseArray));
    }

    auto responseView = bsoncxx::stdx::string_view{rawResponse.data(), rawResponse.size()};
    auto responseAsBson = fromBsoncxxDocument(bsoncxx::from_json(responseView));
    auto jsonResponse = Value(std::move(responseAsBson));
    if (!parseJsonStrings) {
        return jsonResponse;
    }

    auto responseDocument = convertJsonStringsToJson(jsonResponse.getDocument());
    return Value(std::move(responseDocument));
}

}  // namespace streams
