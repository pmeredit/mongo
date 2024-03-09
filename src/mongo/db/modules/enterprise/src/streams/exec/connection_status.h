#pragma once

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "streams/util/exception.h"

namespace streams {

// ConnectionStatus is used to track the connection health of Operators like $source.
struct ConnectionStatus {
    enum Status {
        // Still trying to connect to the target.
        kConnecting,
        // Connected to the target.
        kConnected,
        // Encountered error while connecting to the target.
        kError
    };

    // Status of the connection.
    Status status{kConnecting};
    // Set if status is kError.
    SPStatus error;

    bool isConnecting() {
        return status == kConnecting;
    }

    bool isConnected() {
        return status == kConnected;
    }

    bool isError() {
        return status == kError;
    }
};
};  // namespace streams
