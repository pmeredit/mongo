/**
 *    Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <iosfwd>

#include "mongo/platform/mutex.h"

namespace mongo {

/**
 * Representation of the console.  Use this in place of cout/cin, in applications that write to
 * the console from multiple threads (such as those that use the logging subsystem).
 *
 * The Console type is synchronized such that only one instance may be in the fully constructed
 * state at a time.  Correct usage is to instantiate one, write or read from it as desired, and
 * then destroy it.
 *
 * The console streams accept UTF-8 encoded data, and attempt to write it to the attached
 * console faithfully.
 *
 * TODO(schwerin): If no console is attached on Windows (services), should writes here go to the
 * event logger?
 */
class Console {
public:
    Console();

    std::ostream& out();

private:
    stdx::unique_lock<stdx::mutex> _consoleLock;
};

}  // namespace mongo
