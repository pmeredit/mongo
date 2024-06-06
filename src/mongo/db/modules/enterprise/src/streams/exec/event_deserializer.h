/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

namespace mongo {
class BSONObj;
}  // namespace mongo

namespace streams {

/**
 * This class is the abstract base class of all event deserializers.
 * An event deserializer can be used to deserialize an event read from an input source into a
 * mongo::Document.
 */
class EventDeserializer {
public:
    virtual ~EventDeserializer() = default;

    // Deserializes the event stored in the given buffer into the corresponding mongo::BSONObj.
    // Throws if any errors are encountered.
    mongo::BSONObj deserialize(const char* buf, int len);

protected:
    virtual mongo::BSONObj doDeserialize(const char* buf, int len) = 0;
};

}  // namespace streams
