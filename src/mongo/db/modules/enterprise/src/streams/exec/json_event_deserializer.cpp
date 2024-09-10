/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/json_event_deserializer.h"

#include <bsoncxx/json.hpp>

#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/mongocxx_utils.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

BSONObj JsonEventDeserializer::doDeserialize(const char* buf, int len) {
    if (len == 0) {
        return BSONObj();
    }
    return fromBsoncxxDocument(bsoncxx::from_json(bsoncxx::stdx::string_view{buf, (size_t)len}));
}

}  // namespace streams
