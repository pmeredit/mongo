/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

#include "streams/exec/json_event_deserializer.h"
#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

namespace streams {

using namespace mongo;

BSONObj JsonEventDeserializer::doDeserialize(const char* buf, int len) {
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
