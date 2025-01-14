/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/json_event_deserializer.h"

#include <bsoncxx/exception/exception.hpp>
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

    bsoncxx::stdx::string_view view{buf, (size_t)len};

    // We automatically parse Confluent JSON. Confluent JSON prepends 1 Magic Byte and 4 schema ID
    // bytes, see
    // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#messages-wire-format
    char confluentMagicByte{0};
    int confluentPrefixSize = sizeof(confluentMagicByte) + 4;
    if (len > confluentPrefixSize && buf[0] == confluentMagicByte) {
        view = bsoncxx::stdx::string_view{buf + confluentPrefixSize,
                                          (size_t)len - confluentPrefixSize};
    }

    return fromBsoncxxDocument(bsoncxx::from_json(view));
}

}  // namespace streams
