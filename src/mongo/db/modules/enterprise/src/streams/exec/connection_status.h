#pragma once

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"

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
    mongo::ErrorCodes::Error errorCode;
    // Set if status is kError.
    std::string errorReason;

    bool isConnected() {
        return status == kConnected;
    }
};
};  // namespace streams
