/**
 *    Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "appender.h"
#include "console.h"
#include "encoder.h"
#include "mongo/base/status.h"

namespace mongo {
namespace logger {

/**
 * Appender for writing to the console (stdout).
 */
template <typename Event>
class ConsoleAppender : public Appender<Event> {
public:
    ConsoleAppender() = delete;
    using EventEncoder = Encoder<Event>;
    explicit ConsoleAppender(std::unique_ptr<EventEncoder> encoder)
        : _encoder(std::move(encoder)) {}

    Status append(const Event& event) final {
        Console console;
        _encoder->encode(event, console.out()).flush();
        if (!console.out()) {
            return {ErrorCodes::LogWriteFailed, "Error writing log message to console."};
        }
        return Status::OK();
    }

private:
    std::unique_ptr<EventEncoder> _encoder;
};

}  // namespace logger
}  // namespace mongo
