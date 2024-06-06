/**
 *    Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <iosfwd>

namespace mongo {
namespace logger {

/**
 * Interface for objects that encode Events to std::ostreams.
 *
 * Most appender implementations write to streams, and Encoders represent the process of
 * encoding events into streams.
 */
template <typename Event>
class Encoder {
public:
    virtual ~Encoder() {}
    virtual std::ostream& encode(const Event& event, std::ostream& os) = 0;
};

}  // namespace logger
}  // namespace mongo
