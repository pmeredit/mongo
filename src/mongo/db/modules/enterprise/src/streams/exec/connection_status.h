#pragma once

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

    // Throws an exception if the state is not kConnected.
    // Used by the Executor and SinkOperator classes to ensure the operator is healthy.
    void throwIfNotConnected();
};
};  // namespace streams
