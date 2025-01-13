/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <exception>
#include <string>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"

namespace streams {

// A convenience macro to throw an SPException is expr is not true.
#define spassert(spStatus, expr)     \
    if (!expr) {                     \
        throw SPException(spStatus); \
    };

// Like uasserted, but for SPExceptions.
#define spasserted(spStatus) throw SPException(spStatus)

// SPStatus is just a mongo::Status with an additional field for unsafe error messages.
// It's assumed the reason() in the mongo::Status is safe to return to customers.
class SPStatus : public mongo::Status {
public:
    SPStatus() : mongo::Status{mongo::Status::OK()} {}

    SPStatus(mongo::Status status, std::string unsafeError)
        : mongo::Status{std::move(status)}, _unsafeError(std::move(unsafeError)) {}

    SPStatus(mongo::Status status) : mongo::Status{std::move(status)} {}

    bool operator==(const SPStatus& other) const {
        return _unsafeError == other._unsafeError && code() == other.code() &&
            reason() == other.reason();
    }

    // An error message used for internal logs that should not be sent to customers.
    const std::string& unsafeReason() const {
        return _unsafeError;
    }

private:
    std::string _unsafeError;
};

// Fatal exception thrown by a stream processor. No operators should
// ever attempt to catch this, the exception should be propagated
// up the call stack to the executor.
class SPException : public std::exception {
public:
    SPException(mongo::Status status) : _status(std::move(status)) {}

    SPException(SPStatus spStatus)
        : _status(spStatus), _unsafeErrorMessage(spStatus.unsafeReason()) {}

    const char* what() const noexcept override {
        return reason().c_str();
    }

    const std::string& reason() const {
        return _status.reason();
    }

    mongo::ErrorCodes::Error code() const {
        return _status.code();
    }

    SPStatus toStatus() const {
        return SPStatus(_status, _unsafeErrorMessage);
    }

    std::string toString() const {
        return _status.toString();
    }

    const std::string& unsafeReason() const {
        return _unsafeErrorMessage;
    }

private:
    // Status received
    mongo::Status _status{mongo::Status::OK()};

    // An error message that is not safe to return to customers.
    std::string _unsafeErrorMessage;
};  // class SPException


// Same as mongo::exceptionToStatus(), but also catches SPException.
// Also, it returns InternalError in place of UnknownError.
SPStatus exceptionToSPStatus() noexcept;

};  // namespace streams
