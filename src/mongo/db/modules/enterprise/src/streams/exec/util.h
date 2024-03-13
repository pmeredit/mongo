#pragma once

#include <boost/optional.hpp>
#include <bsoncxx/document/value.hpp>

#include "streams/exec/constants.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/message.h"
#include "streams/exec/stages_gen.h"

namespace mongo {
class BSONObjBuilder;
}  // namespace mongo

namespace streams {

bool isSourceStage(mongo::StringData name);
bool isSinkStage(mongo::StringData name);
bool isWindowStage(mongo::StringData name);
bool isLookUpStage(mongo::StringData name);
bool isEmitStage(mongo::StringData name);
bool isMergeStage(mongo::StringData name);
bool isWindowAwareStage(mongo::StringData name);

int64_t toMillis(mongo::StreamTimeUnitEnum unit, int count);

// Builds a DLQ message for the given StreamMeta.
mongo::BSONObjBuilder toDeadLetterQueueMsg(const boost::optional<std::string>& streamMetaFieldName,
                                           mongo::StreamMeta streamMeta,
                                           boost::optional<std::string> error);

// Builds a DLQ message for the given StreamDocument.
mongo::BSONObjBuilder toDeadLetterQueueMsg(const boost::optional<std::string>& streamMetaFieldName,
                                           StreamDocument streamDoc,
                                           boost::optional<std::string> error);

// Gets the namespace string for the given 'db' and 'coll' literals.
mongo::NamespaceString getNamespaceString(const std::string& dbStr, const std::string& collStr);

// Gets the namespace literal for the given 'db' and 'coll' name expressions.
mongo::NamespaceString getNamespaceString(const mongo::NameExpression& db,
                                          const mongo::NameExpression& coll);

// Returns whether or not the input `status` is a retryable error.
bool isRetryableStatus(const mongo::Status& status);

// Returns the single index in the writeErrors field in the given exception returned by mongocxx.
mongo::write_ops::WriteError getWriteErrorIndexFromRawServerError(
    const bsoncxx::document::value& rawServerError);

// Add the stream meta values to the stream meta field in the document.
mongo::Document updateStreamMeta(const mongo::Value& streamMetaInDoc,
                                 const mongo::StreamMeta& internalStreamMeta);

}  // namespace streams
