#pragma once

#include <fmt/format.h>

#include "streams/exec/context.h"

namespace streams {

// A convenience macro to assert during checkpoint read operations.
// TODO(SERVER-78501): Add specific error codes in checkpoint assertions.
#define CHECKPOINT_RECOVERY_ASSERT(checkpointId, operatorId, msg, assertion)                     \
    uassert(ErrorCodes::InternalError,                                                           \
            fmt::format("checkpointId: {} operatorId: {} encountered error during recovery: {}", \
                        checkpointId,                                                            \
                        operatorId,                                                              \
                        msg),                                                                    \
            assertion);

// A convenience macro to assert during checkpoint write operations.
#define CHECKPOINT_WRITE_ASSERT(checkpointId, operatorId, msg, assertion)                     \
    uassert(ErrorCodes::InternalError,                                                        \
            fmt::format("checkpointId: {} operatorId: {} encountered error during write: {}", \
                        checkpointId,                                                         \
                        operatorId,                                                           \
                        msg),                                                                 \
            assertion);

// This function allows Context* to be used in LOG statements.
mongo::BSONObj toBSON(Context* context);

}  // namespace streams
