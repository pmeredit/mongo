/**
 *    Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#ifndef _WIN32  // TODO(schwerin): Should be #if MONGO_CONFIG_HAVE_SYSLOG_H?

#include <sstream>
#include <syslog.h>

#include "appender.h"
#include "encoder.h"
#include "mongo/base/status.h"

namespace mongo {
namespace logger {

/**
 * Appender for writing to syslog.  Users must have separately called openlog().
 */
template <typename Event>
class SyslogAppender : public Appender<Event> {
public:
    using EventEncoder = Encoder<Event>;
    explicit SyslogAppender(std::unique_ptr<EventEncoder> encoder) : _encoder(std::move(encoder)) {}
    Status append(const Event& event) final {
        std::ostringstream os;
        _encoder->encode(event, os);
        if (!os) {
            return {ErrorCodes::LogWriteFailed, "Error writing log message to syslog."};
        }
        syslog(LOG_INFO, "%s", os.str().c_str());
        return Status::OK();
    }

private:
    std::unique_ptr<EventEncoder> _encoder;
};

}  // namespace logger
}  // namespace mongo

#endif
