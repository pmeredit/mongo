/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <bsoncxx/document/value.hpp>
#include <mongocxx/exception/exception.hpp>

#include "streams/exec/constants.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/message.h"
#include "streams/exec/stages_gen.h"

namespace mongo {
class BSONObjBuilder;
}  // namespace mongo

namespace streams {

// TODO(SERVER-97571): Move these planner specific utilities to a separate file.
using BSONPipeline = std::vector<mongo::BSONObj>;
bool isSourceStage(mongo::StringData name);
bool isSinkStage(mongo::StringData name);
bool isSinkOnlyStage(mongo::StringData name);
bool isMiddleAndSinkStage(mongo::StringData name);
bool isWindowStage(mongo::StringData name);
bool hasWindow(const std::vector<mongo::BSONObj>& pipeline);
bool hasPayloadStage(const std::vector<mongo::BSONObj>& pipeline);
bool hasPayloadStageBeforeWindow(const std::vector<mongo::BSONObj>& pipeline);
bool isLookUpStage(mongo::StringData name);
bool isEmitStage(mongo::StringData name);
bool isMergeStage(mongo::StringData name);
bool isLimitStage(mongo::StringData name);
bool isWindowAwareStage(mongo::StringData name);
bool isBlockingWindowAwareStage(mongo::StringData name);
bool hasBlockingStage(const BSONPipeline& pipeline);
bool isPayloadStage(mongo::StringData name);
bool isExternalFunctionStage(mongo::StringData name);

int64_t toMillis(mongo::StreamTimeUnitEnum unit, int count);

// Builds a DLQ message for the given StreamMeta.
mongo::BSONObjBuilder toDeadLetterQueueMsg(const boost::optional<std::string>& streamMetaFieldName,
                                           const mongo::StreamMeta& streamMeta,
                                           const std::string& operatorName,
                                           boost::optional<std::string> error);

// Builds a DLQ message for the given StreamDocument.
mongo::BSONObjBuilder toDeadLetterQueueMsg(const boost::optional<std::string>& streamMetaFieldName,
                                           const StreamDocument& streamDoc,
                                           const std::string& operatorName,
                                           boost::optional<std::string> error);

// Builds a DLQ message for the given StreamMeta and bson document
mongo::BSONObjBuilder toDeadLetterQueueMsg(const boost::optional<std::string>& streamMetaFieldName,
                                           const mongo::StreamMeta& streamMeta,
                                           const mongo::Document& doc,
                                           const std::string& operatorName,
                                           boost::optional<std::string> error);

// Builds a DLQ message for the given Status
mongo::BSONObjBuilder toDeadLetterQueueMsg(const boost::optional<std::string>& streamMetaFieldName,
                                           const StreamDocument& streamDoc,
                                           const std::string& operatorName,
                                           mongo::Status status);

// Gets the namespace string for the given 'db' and 'coll' literals.
mongo::NamespaceString getNamespaceString(const std::string& dbStr, const std::string& collStr);

// Gets the namespace literal for the given 'db' and 'coll' name expressions.
mongo::NamespaceString getNamespaceString(const mongo::NameExpression& db,
                                          const mongo::NameExpression& coll);

// Returns whether or not the input `status` is a retryable error.
bool isRetryableStatus(const mongo::Status& status);

// Add the stream meta values to the stream meta field in the document.
mongo::Document updateStreamMeta(const mongo::Value& streamMetaInDoc,
                                 const mongo::StreamMeta& internalStreamMeta);

// Determines if a list of bootstrap servers represents a confluent broker
bool isConfluentBroker(const std::string& bootstrapServers);

// Returns an anonymized pipeline for logging purposes
std::vector<mongo::StringData> getLoggablePipeline(const std::vector<mongo::BSONObj>& pipeline);

// Change all Date fields to ISO8601 strings.
mongo::Document convertDateToISO8601(mongo::Document doc);

// Change all json strings to json for the provided document.
mongo::Document convertJsonStringsToJson(mongo::Document doc);

// Change all json strings to json for the provided array.
std::vector<mongo::Value> jsonStringToValue(const std::vector<mongo::Value>& arr);

// Generic helper function for recursively converting fields for a Document
mongo::Document convertAllFields(mongo::Document doc,
                                 std::function<mongo::Value(const mongo::Value&)> convertFunc);

// Generic helper function for recursively converting fields for an array
std::vector<mongo::Value> convertAllFields(
    const std::vector<mongo::Value>& arr,
    std::function<mongo::Value(const mongo::Value&)> convertFunc);

// parseAndDeserializeResponse will convert a json response to a mongo value. If specified, any json
// strings embedded within will also be parsed
mongo::Value parseAndDeserializeJsonResponse(mongo::StringData rawResponse, bool parseJsonStrings);

}  // namespace streams
