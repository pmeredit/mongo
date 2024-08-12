/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/platform/mutex.h"
#include <string>

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
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("KafkaCallbackHelper::mutex");
    std::deque<std::string> _errorBuffer;
};

}  // namespace streams
