/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/event_deserializer.h"

namespace streams {

/**
 * An event deserializer that can deserialize an input event that is in JSON format.
 */
class JsonEventDeserializer : public EventDeserializer {
public:
    JsonEventDeserializer() {}

private:
    mongo::BSONObj doDeserialize(const char* buf, int len) override;
};

}  // namespace streams
