/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <boost/algorithm/hex.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/join.hpp>
#include <cerrno>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <termios.h>

#include "streams/exec/kafka_callback_base.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;

namespace {
constexpr int kMaxErrorBufferSize = 5;
}

namespace streams {

void KafkaCallbackBase::addUserFacingError(std::string error) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    if (std::find(_errorBuffer.begin(), _errorBuffer.end(), error) != _errorBuffer.end()) {
        // Duplicate of existing error, skip this one
        return;
    }

    if (_errorBuffer.size() == kMaxErrorBufferSize) {
        _errorBuffer.pop_front();
    }
    _errorBuffer.push_back(std::move(error));
}

std::string KafkaCallbackBase::getAllErrorsAsString() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    return boost::algorithm::join(_errorBuffer, ", ");
}

bool KafkaCallbackBase::hasErrors() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    return !_errorBuffer.empty();
}

}  // namespace streams
