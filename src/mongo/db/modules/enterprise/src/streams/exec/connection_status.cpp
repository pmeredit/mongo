/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/connection_status.h"
#include "mongo/base/error_codes.h"
#include "mongo/util/assert_util.h"

namespace streams {

using namespace mongo;

void ConnectionStatus::throwIfNotConnected() {
    if (status == kError) {
        spasserted(std::move(error));
    } else if (status == kConnecting) {
        // This is an internal error, it's not currently expected to happen.
        uasserted(ErrorCodes::InternalError, "Expected connection status to be kConnected");
    }
}

}  // namespace streams
