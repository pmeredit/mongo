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
bool isSourceStage(mongo::StringData name);
bool isSinkStage(mongo::StringData name);
bool isWindowStage(mongo::StringData name);
bool hasWindow(const std::vector<mongo::BSONObj>& pipeline);
bool hasHttpsStage(const std::vector<mongo::BSONObj>& pipeline);
bool hasHttpsStageBeforeWindow(const std::vector<mongo::BSONObj>& pipeline);
bool isLookUpStage(mongo::StringData name);
bool isEmitStage(mongo::StringData name);
bool isMergeStage(mongo::StringData name);
bool isWindowAwareStage(mongo::StringData name);
bool isBlockingWindowAwareStage(mongo::StringData name);
bool isHttpsStage(mongo::StringData name);

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

}  // namespace streams
