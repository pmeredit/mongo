/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "appender.h"
#include "console.h"
#include "encoder.h"
#include "mongo/base/status.h"
#include "mongo/db/audit.h"

namespace mongo {

namespace logger {

/**
 * Mock appender for testing audit logging.
 */
template <typename Encoder>
class MockAppender : public Appender<audit::AuditInterface::AuditEvent> {
public:
    MockAppender() = delete;
    explicit MockAppender(std::unique_ptr<Encoder> encoder) : _encoder(std::move(encoder)) {}

    Status append(const audit::AuditInterface::AuditEvent& event) final {
        _out = _encoder->encode(event);
        return Status::OK();
    }

    StringData getLast() {
        if (_out.empty()) {
            return ""_sd;
        }
        return _out;
    }

private:
    std::unique_ptr<Encoder> _encoder;
    std::string _out;
};

}  // namespace logger

}  // namespace mongo
