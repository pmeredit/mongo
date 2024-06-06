/**
 *    Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/base/status.h"

namespace mongo {
namespace logger {

/**
 * Interface for sinks in a logging system.  The core of logging is when events of type E are
 * appended to instances of Appender<E>.
 *
 * Example concrete instances are ConsoleAppender<E>, SyslogAppender<E> and
 * RotatableFileAppender<E>.
 */
template <typename E>
class Appender {
public:
    typedef E Event;

    virtual ~Appender() {}

    /**
     * Appends "event", returns Status::OK() on success.
     */
    virtual Status append(const Event& event) = 0;

    /**
     * Perform log rotation (if applicable)
     */
    virtual Status rotate(bool renameFiles,
                          StringData suffix,
                          std::function<void(Status)> onMinorError) {
        return Status::OK();
    }
};

}  // namespace logger
}  // namespace mongo
