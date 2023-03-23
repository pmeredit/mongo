#pragma once

#include "streams/exec/event_deserializer.h"

namespace streams {

/**
 * An event deserializer that can deserialize an input event that is in JSON format.
 */
class JsonEventDeserializer : public EventDeserializer {
private:
    mongo::BSONObj doDeserialize(const char* buf, int len) override;
};

}  // namespace streams
