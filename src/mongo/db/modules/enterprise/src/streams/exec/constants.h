#pragma once

namespace streams {

enum ErrorCode {
    // TODO: error codes and messages
    kTemporaryUserErrorCode = 99999,
    kTemoraryInternalErrorCode = 100000,
    kTemporaryLoggingCode = 100001
};

constexpr const char kStreamsMetaField[] = "_stream_meta";

};  // namespace streams
