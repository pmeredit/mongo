/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <queue>
#include <string>

#include "mongo/stdx/mutex.h"

namespace streams {

struct Context;

class KafkaCallbackBase {
public:
    virtual ~KafkaCallbackBase() = default;

    bool hasErrors();
    std::string getAllErrorsAsString();

protected:
    // addUserFacingError should only receive "safe" errors which we can directly
    // propagate to users. This only stores unique errors - if we've already
    // logged a particular error, it's not added to the error queue.
    void addUserFacingError(std::string error);

    // Protects access to the error messages buffer.
    mutable mongo::stdx::mutex _mutex;
    std::deque<std::string> _errorBuffer;
};

}  // namespace streams
