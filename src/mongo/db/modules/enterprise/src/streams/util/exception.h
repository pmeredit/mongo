#pragma once

#include <exception>
#include <string>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"

namespace streams {

// Fatal execption thrown by a stream processor. No operators should
// ever attempt to catch this, the exception should be propagated
// up the call stack to the executor.
class SPException : public std::exception {
public:
    SPException(mongo::Status status) : _status(std::move(status)) {}

    const char* what() const noexcept override {
        return reason().c_str();
    }

    const std::string& reason() const {
        return _status.reason();
    }

    mongo::ErrorCodes::Error code() const {
        return _status.code();
    }

    const mongo::Status& toStatus() const {
        return _status;
    }

    std::string toString() const {
        return _status.toString();
    }

private:
    // Status received
    mongo::Status _status{mongo::Status::OK()};
};  // class SPException

};  // namespace streams
