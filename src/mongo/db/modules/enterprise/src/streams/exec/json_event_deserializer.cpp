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
    // Check for any UTF-16 escape sequences.
    if (_options.allowBsonCxxParsing &&
        (_options.forceBsonCxxParsing ||
         std::string_view(buf, len).find("\\u") != std::string::npos)) {
        // JSON strings can contain UTF-16 escape sequences, according to
        // https://datatracker.ietf.org/doc/html/rfc8259#section-7.
        // However fromjson does not support this. So, we use bsoncxx to parse the JSON string.
        // We create a BSONObj from the bsoncxx::document.
        return fromBsoncxxDocument(
            bsoncxx::from_json(bsoncxx::stdx::string_view{buf, (size_t)len}));
    }

    int actualLen{0};
    auto obj = fromjson(buf, &actualLen);
    while (actualLen < len) {
        if (std::isspace(buf[actualLen])) {
            ++actualLen;
            continue;
        }
        uasserted(ErrorCodes::InvalidOptions,
                  str::stream() << "Unexpected extra character in the message: "
                                << int(buf[actualLen]));
    }
    return obj;
}

}  // namespace streams
